import json
import logging
import shlex
import subprocess
import threading
from collections.abc import Iterable
from contextlib import contextmanager
from datetime import datetime
from typing import List, TypeVar, Sequence, Optional, Type, Generator

import psycopg
from psycopg import Cursor
from psycopg.rows import class_row, dict_row

from src.api import YTApi, HttpError
from src.channel import BaseChannel
from src.config import Config, Script
from src.db import models
from src.db.utils import execute_values
from src.downloaders import thumbnail, video as video_downloader
from src.enum import Site
from src.playlist import YTPlaylist
from src.video import SITE_CLASSES, BaseVideo

logger = logging.getLogger('debug')
T = TypeVar('T')
BaseVideoT = TypeVar('BaseVideoT', bound=BaseVideo)


class PlaylistChecker:
    def __init__(self, config: Config = None):
        if not config:
            config = Config.load()

        self.config = config

        self.already_checked = {site: set() for site in list(Site.__members__.values())}
        # Dict of dicts in the form {site: {video_id: db_id}}
        self.all_vids = {site: {} for site in list(Site.__members__.values())}
        self.all_vid_ids = {site: set() for site in list(Site.__members__.values())}
        self.channel_cache = {site: set() for site in list(Site.__members__.values())}
        self.db_channel_cache = {site: set() for site in list(Site.__members__.values())}

        self._conn = None

        self._yt_api = YTApi(self.config.yt_token)
        self.all_tags = {}
        self.threads = []

    @staticmethod
    def with_connection(fn):
        def wrapper(self, *args, **kwargs):
            with psycopg.connect(self.config.db_conn_string,
                                 row_factory=dict_row) as conn:
                self._conn = conn
                fn(self, *args, **kwargs)

        return wrapper

    @property
    def conn(self) -> psycopg.Connection:
        if self._conn is None:
            raise ValueError('Connection was not initialized')

        return self._conn

    @contextmanager
    def class_cursor(self, cls: Type[T]) -> Cursor[T]:
        with self.conn.cursor(row_factory=class_row(cls)) as cur:
            yield cur

    @property
    def yt_api(self):
        return self._yt_api

    def update_archived_playlist_videos(self, playlist_id: int):
        """
        Use when archive property is set to true on a playlist.
        Will set the download flag to true on each video in the playlist
        """
        sql = '''
        UPDATE videos v SET download=TRUE
        FROM playlistvideos pv
        WHERE pv.playlist_id=%s AND v.id=pv.video_id AND v.download=FALSE
        '''

        with self.conn.transaction():
            with self.conn.cursor() as cur:
                cur.execute(sql, (playlist_id,))
                logger.info(f'Updated archive {cur.rowcount}')

    def add_and_update_vids(self, videos: Iterable[BaseVideo], site: int | Site):
        """
        Adds new videos to database and updates the following properties from
        duplicate entries:
            title, description, thumbnail url, publish date
            also sets the deleted flag to False

        Do not give deleted videos to this function. It will set the deleted
        flag to true on them

        Args:
            videos (collections.Iterable of src.video.BaseVideo):
                Iterable of :class:BaseVideo that will be added or updated
            site (int):
                id of the site being used
        """
        videos_set = set(videos)
        # We can't use intersection because it wouldn't give our custom object
        # instead it always would give a set of values used for comparison
        do_insert = videos_set - self.all_vid_ids[site]
        do_update = videos_set - do_insert

        if not (do_insert or do_update):
            return

        site = int(site)

        with self.conn.transaction():
            if do_insert:
                sql = 'INSERT INTO videos AS v (video_id, title, description, published_at, site, thumbnail) ' \
                      'VALUES %s'

                values = tuple(
                    (vid.video_id, vid.title, vid.description, vid.published_at, site, vid.thumbnail)
                    for vid in do_insert
                )

                with self.conn.cursor() as cursor:
                    execute_values(cursor, sql, values, page_size=500)

            if do_update:
                # https://stackoverflow.com/a/18799497/6046713
                sql = 'UPDATE videos AS v SET ' \
                      'title=CASE WHEN v.title!=c.title THEN c.title ELSE v.title END, ' \
                      'description=CASE WHEN v.description!=c.description THEN c.description ELSE v.description END, ' \
                      'deleted=FALSE,' \
                      'thumbnail=COALESCE(c.thumbnail, v.thumbnail), ' \
                      'published_at=CASE WHEN c.published_at >= v.published_at THEN v.published_at ELSE c.published_at END ' \
                      'FROM (VALUES %s) AS c(video_id, title, description, published_at, site, thumbnail) ' \
                      'WHERE c.site=v.site AND c.video_id=v.video_id'

                values = tuple(
                    (vid.video_id, vid.title, vid.description, vid.published_at, site, vid.thumbnail)
                    for vid in do_update
                )

                with self.conn.cursor() as cursor:
                    execute_values(cursor, sql, values, page_size=500)

    def add_deleted_vids(self, videos: Iterable[BaseVideo], site: int | Site):
        """
        Sets the deleted flag on the videos provided and also sets the
        deletion time column if the deleted flag hasn't been set before

        Args:
            videos:
                Iterable of BaseVideo that are deleted
            site:
                id of the site being used
        """
        videos = set(videos)
        do_insert = videos - self.all_vid_ids[site]
        do_update = videos - do_insert

        if not (do_insert or do_update):
            return

        site = int(site)
        with self.conn.transaction():
            if do_insert:
                t = datetime.utcnow()
                sql = 'INSERT INTO videos (video_id, title, published_at, site, deleted, deleted_at) VALUES %s'

                values = tuple((vid.video_id, t, t) for vid in do_insert)

                with self.conn.cursor() as cursor:
                    execute_values(cursor, sql, values, page_size=1000,
                                   template=f"(%s, 'Deleted video', %s, {site}, True, %s)")

            if do_update:
                sql = 'UPDATE videos AS v SET ' \
                      'deleted_at=CASE WHEN v.deleted=FALSE THEN CURRENT_TIMESTAMP ELSE v.deleted_at END, ' \
                      'deleted=TRUE ' \
                      'WHERE site=%s AND video_id=ANY(%s)'

                with self.conn.cursor() as cursor:
                    cursor.execute(sql, [site, [v.video_id for v in do_update]])

    def add_vid_tags(self, videos: list[BaseVideo], site: int | Site, default_tags: list[str] = None):
        """
        Adds missing tags to the database based on the provided videos

        Args:
            videos:
                List of videos from which the tags will be added
            site:
                id of the site being used
            default_tags:
                An list of tag names to be applied to every video in the videos
                param
        """
        site = int(site)
        default_tags = [] if not default_tags else default_tags
        values = set(default_tags)
        cached_tags = set(self.all_tags.keys())

        # Copy the list of videos since we don't wanna edit the original list
        videos = videos.copy()
        for vid in videos.copy():  # This copy is probably needed
            if not vid.data:
                videos.remove(vid)
                continue

            tags = vid.tags
            if not tags:
                videos.remove(vid)
                continue

            values.update(map(str.lower, tags))

        # Get non cached tags and add them to db
        tobecached = values - cached_tags

        if tobecached:
            sql = 'INSERT INTO tags (tag) VALUES %s ON CONFLICT DO NOTHING RETURNING tag, id'
            try:
                with self.class_cursor(models.Tag) as cursor:
                    results: list[models.Tag] = execute_values(cursor, sql,
                                                               [(x,) for x in tobecached],
                                                               page_size=1000, fetch=True)

                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

            for tag in results:
                self.all_tags[tag.tag] = tag.id

        values = []
        for vid in videos:
            video_id = self.all_vids[site].get(vid.video_id)
            if not video_id:
                logger.warning('Video id not found with %s' % vid)
                continue

            # Add video specific tags
            for tag in vid.tags:
                tag_id = self.all_tags.get(tag.lower())
                if not tag_id:
                    logger.warning('Tag %s not found' % tag)
                    continue

                values.append((tag_id, video_id))

            # Add default tags
            for default_tag in default_tags:
                tag_id = self.all_tags.get(default_tag.lower())
                if not tag_id:
                    logger.warning('Tag %s not found' % default_tag)
                    continue

                values.append((tag_id, video_id))

        sql = 'INSERT INTO videoTags (tag_id, video_id) VALUES %s ON CONFLICT DO NOTHING '

        try:
            with self.conn.cursor() as cursor:
                execute_values(cursor, sql, values, page_size=2000)

            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def add_channels(self, channels: Iterable[BaseChannel], site: int | Site):
        """
        Adds channels to db and updates old entries
        Columns updated are as follows:
            name and thumbnail aka profile pic

        Args:
            channels: iterable of channels to add
            site: id of the site

        """
        site = int(site)
        channels = set(channels)
        do_insert = channels - self.db_channel_cache[site]
        do_update = channels - do_insert

        if not (do_insert or do_update):
            return

        with self.conn.transaction():
            if do_insert:
                sql = 'INSERT INTO channels (channel_id, name, thumbnail, site) VALUES %s'

                with self.conn.cursor() as cursor:
                    execute_values(cursor, sql, [(c.channel_id, c.name, c.thumbnail, site) for c in do_insert], page_size=1000)

                self.db_channel_cache[site].update([c.channel_id for c in do_insert])

            if do_update:
                sql = 'UPDATE channels AS c SET ' \
                      'name=COALESCE(v.name, c.name), ' \
                      'thumbnail=COALESCE(v.thumbnail, c.thumbnail) ' \
                      'FROM (VALUES %s) AS v(channel_id, name, thumbnail) ' \
                      'WHERE v.channel_id=c.channel_id'

                with self.conn.cursor() as cursor:
                    execute_values(cursor, sql, [(c.channel_id, c.name, c.thumbnail) for c in do_update], page_size=1000)

    def add_channel_videos(self, videos: Iterable[BaseVideo], channels: Sequence[BaseChannel | str], site: int | Site):
        """
        Link video ids to channel ids in the channelVideos table
        This will handle adding missing channels for you. The videos need
        to have the channel property set to for this to work

        Args:
            videos:
                List of :class:BaseVideo instances
            channels:
                List of BaseChannel instances and channel_ids as str
            site:
                id of the site being used
        """
        site = int(site)
        self.add_channels([c for c in channels if not isinstance(c, str)], site)
        sql = 'SELECT id, channel_id FROM channels WHERE channel_id=ANY(%s)'

        channel_ids = {}
        with self.class_cursor(models.Channel) as cursor:
            cursor.execute(sql, [[c if isinstance(c, str) else c.channel_id for c in channels]])

            for row in cursor:
                channel_ids[row.channel_id] = row.id

        data = []

        for vid in videos:
            channel_id = channel_ids.get(vid.channel_id)
            if not channel_id:
                logger.warning(f'Channel not found for video {vid}')
                continue

            vid_id = self.all_vids[site].get(vid.video_id)
            if not vid_id:
                continue

            data.append((channel_id, vid_id))

        try:
            sql = 'INSERT INTO channelVideos (channel_id, video_id) VALUES %s ON CONFLICT DO NOTHING'
            with self.conn.cursor() as cursor:
                execute_values(cursor, sql, data, page_size=2000)

            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def add_playlist_vids(self, playlist_id: int, video_ids: Iterable[int]):
        """
        Add video playlist connection to the playlistVideos table

        Args:
            playlist_id:
                The database id for the playlist
            video_ids:
                An iterable of database ids for videos that are added the
                specified playlist
        """
        sql = 'INSERT INTO playlistVideos (playlist_id, video_id) VALUES ' \
              '%s ON CONFLICT DO NOTHING'

        values = tuple((playlist_id, video_id) for video_id in video_ids)

        try:
            with self.conn.cursor() as cursor:
                execute_values(cursor, sql, values, page_size=2000)

            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def update_removed_playlist_videos(self, playlist_id: int, video_ids: list[int]):
        """
        Removes playlist videos that are not found in the video_ids iterable.
        """
        sql = 'DELETE FROM playlistvideos WHERE playlist_id=%s AND NOT video_id=ANY(%s)'

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, [playlist_id, video_ids])
                logger.info(f'user removed {cursor.rowcount} videos from playlist {playlist_id}')

            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def get_vid_ids(self, vid_ids: list[str], site: int | Site) -> dict[str, int]:
        """
        Gets the database ids to the corresponding video ids

        Args:
            vid_ids:
                list of video ids of the specified site.
            site:
                ID of the site being used

        Returns:
            dict: a dictionary of type {str: int} aka {video_id: database_id}
        """
        site = int(site)
        sql = f'SELECT id, video_id, site FROM videos WHERE site={site} AND video_id=ANY(%s)'

        with self.class_cursor(models.PartialVideo) as cursor:
            cursor.execute(sql, [vid_ids])
            vid_ids = {vid.video_id: vid.id for vid in cursor}

        self.all_vids[site].update(vid_ids)
        self.all_vid_ids[site].update(vid_ids.keys())
        return vid_ids

    def add_playlist(self, playlist_id: str, name: str, site: int | Site) -> int:
        """
        Adds a playlist to the database if it doesn't exist

        Args:
            playlist_id:
                id of the playlist
            name:
                name of the playlist
            site:
                ID of the site being used

        Returns:
            int: The database id of the newly made playlist

        """
        site = int(site)
        sql = 'INSERT INTO playlists (playlist_id, name, site) VALUES (%s, %s, %s) RETURNING id'

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, (playlist_id, name, int(site)))
                playlist_id = cursor.fetchone()[0]

            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

        return playlist_id

    def get_playlist_video_ids(self, playlist_id: int) -> list[models.PlaylistVideo]:
        """
        Gets all video ids that are associated with this playlist
        Args:
            playlist_id: id of the playlist
        Returns:
            list:
                A list PlaylistVideo objects with the video_id property set
        """
        sql = 'SELECT video_id FROM playlistVideos WHERE playlist_id=%s'

        with self.class_cursor(models.PlaylistVideo) as cursor:
            cursor.execute(sql, (playlist_id,))
            return cursor.fetchall()

    def iter_videos_to_download(self, playlist_ids: list[int] = None) -> Generator[models.Video, None, None]:
        where = '((download=TRUE or force_redownload=TRUE) AND deleted=FALSE)'
        join = ''
        args = ()

        if playlist_ids:
            where = 'pv.playlist_id=ANY(%s) AND ' + where
            join = 'INNER JOIN playlistvideos pv ON v.id = pv.video_id'
            args = [playlist_ids]

        sql = f'''
        SELECT site, id, v.video_id, downloaded_format, downloaded_filename, download_format, force_redownload
        FROM videos v
        {join}
        WHERE {where}
        '''

        with self.class_cursor(models.Video) as cursor:
            cursor.execute(sql, args)
            for row in cursor:
                yield row

    def update_vid_filename(self, filename: Optional[str], downloaded_format: Optional[str], video_id: int):
        sql = 'UPDATE videos SET downloaded_filename=%s, downloaded_format=%s, force_redownload=FALSE WHERE id=%s'

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, (filename, downloaded_format, video_id))

            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    @staticmethod
    def run_after(fields: dict, optional_fields: dict, cmds: List[Script]):
        """
        Runs all specified commands and inputs data encoded in utf-8 to stdin
        """
        for after in cmds:
            logger.info(f'Running script "{after.name}"')
            cmd = after.script

            if not after.required_fields:
                data = {
                    **fields,
                    **optional_fields
                }
            else:
                data = {
                    **fields
                }
                for required_field in after.required_fields:
                    data[required_field.value] = optional_fields[required_field.value]

            try:
                p = subprocess.Popen(shlex.split(cmd), stdin=subprocess.PIPE)
            except FileNotFoundError:
                logger.exception('File "%s" not found' % cmd)
                continue

            p.stdin.write(
                json.dumps(data, ensure_ascii=False).encode('utf-8')
            )
            try:
                out, err = p.communicate()
            except:
                logger.exception('Failed to run script %s' % after)
            else:
                if out:
                    logger.info(out)
                if err:
                    logger.error(err)

    def get_new_deleted(self, deleted: list[BaseVideo], site: int | Site) -> set[BaseVideo]:
        """
        Gets the newly deleted videos from the specified site with
        updated titles

        Args:
            deleted:
                List of all deleted vids from a site.
            site:
                id if the site currently in use

        Returns:
            set: A set of BaseVideo objects with updated titles
        """
        if not deleted:
            return set()

        site = int(site)
        sql = f'SELECT id, title, video_id FROM videos WHERE deleted IS FALSE AND site=%s' \
               ' AND video_id=ANY(%s)'

        new_deleted = set()
        with self.class_cursor(models.Video) as cursor:
            cursor.execute(sql, [site, [vid.video_id for vid in deleted]])

            for video_partial in cursor:
                video_id = video_partial.video_id
                video = None
                for vid in deleted:
                    if vid.video_id == video_id:
                        video = vid
                        break

                if not video:
                    continue

                video.title = video_partial.title
                new_deleted.add(video)

        return new_deleted

    def get_deleted_info(self, deleted: list[BaseVideoT], site: int | Site) -> list[BaseVideoT]:
        """
        Updates BaseVideo objects with cached info from database
        Namely updates title, channel name and channel id

        Args:
            deleted:
                 List of the videos to be updated
            site:
                Id of the site used

        Returns:
            list: Exactly the same list as it was given

        """
        if not deleted:
            return deleted

        site = int(site)
        sql = 'SELECT v.video_id, v.title, c.name, c.channel_id FROM videos v INNER JOIN channelVideos cv ' \
              'ON cv.video_id=v.id INNER JOIN channels c ON cv.channel_id = c.id ' \
             f'WHERE v.site=%s AND v.video_id=ANY(%s)'

        with self.conn.cursor() as cursor:
            cursor.execute(sql, [site, [vid.video_id for vid in deleted]])

            for row in cursor:
                video_id = row['video_id']
                video = None
                for vid in deleted:
                    if vid.video_id == video_id:
                        video = vid
                        break

                if not video:
                    continue

                video.title = row['title']
                video.channel_name = row['name']
                video.channel_id = row['channel_id']

        return deleted

    @with_connection
    def check_all(self, whitelist: list[str] = None):
        """
        Main function of this class that runs the whole thing and
        does all the stuff to make everything work as intended

        Args:
            whitelist (list of str):
                Optional list of playlist ids if you want to only check
                specific playlists
        """
        logger.info('Starting check')
        with self.class_cursor(models.Playlist) as cur:
            sql = 'SELECT * FROM playlists'
            cur.execute(sql)
            _playlists: dict[str, models.Playlist] = {data.playlist_id: data for data in cur}

        with self.class_cursor(models.PartialVideo) as cur:
            sql = 'SELECT id, video_id, site FROM videos'
            cur.execute(sql)

            for vid in cur:
                self.all_vids[vid.site][vid.video_id] = vid.id
                self.all_vid_ids[vid.site].add(vid.video_id)

        with self.class_cursor(models.Tag) as cur:
            # Put all existing tags to cache
            sql = 'SELECT * FROM tags'
            cur.execute(sql)
            for tag in cur:
                self.all_tags[tag.tag] = tag.id

        with self.class_cursor(models.Channel) as cur:
            # Put all inserted channel ids to cache
            sql = 'SELECT id, site, channel_id, name FROM channels'
            cur.execute(sql)
            for channel in cur:
                self.db_channel_cache[channel.site].add(channel.channel_id)

        playlists = self.config.playlists
        logger.info(f'Checking a total of {len(playlists)} playlists')
        checked_playlists: set[int] = set()

        for idx, playlist in enumerate(playlists):
            playlist_id = playlist.playlist_id

            # Ignore non whitelisted playlists if whitelist in use
            if whitelist and playlist_id not in whitelist:
                continue

            logger.info(f'Processing {idx+1}/{len(playlists)} {playlist.name}')

            playlist_row = _playlists.get(playlist_id, None)
            site = playlist.site
            logger.info(f'Checking playlist {playlist_id} on site {site}')

            # Create playlist by site
            if site == Site.Youtube:
                playlist_checker = YTPlaylist(self.conn, self.yt_api, playlist_id)
                if not playlist_row:
                    logger.info('New playlist getting playlist info')
                    info = playlist_checker.get_playlist_info()
                    if not info:
                        continue

                    playlist_row.id = self.add_playlist(playlist_id, info['snippet']['title'], site)
                    playlist_row.name = info['snippet']['title']
            else:
                logger.warning(f'{site} not implemented')
                continue

            # Get videos
            logger.debug('getting old ids')
            old = self.get_playlist_video_ids(playlist_row.id)
            logger.debug('Getting items from youtube')

            try:
                # Items contains undeleted videos
                items, deleted, already_checked = playlist_checker.get_videos(self.already_checked[site])
            except HttpError:
                # Skip on playlist http error
                continue

            thumbnail.bulk_download_thumbnails(items, site)

            # Get new deleted videos
            new_deleted = self.get_new_deleted(deleted, site)

            # Update video cache
            self.already_checked[site].update(items)
            self.already_checked[site].update(deleted)

            # Add new vids to db and update old items
            logger.debug('Adding and updating vids')
            self.add_and_update_vids(items, site)

            # Put all vids in the playlist to a single list
            # in order get the db ids, so we can update
            # the playlistVideos table correctly
            playlist_items = [item.video_id for item in items]
            playlist_items.extend([vid.video_id for vid in deleted])
            playlist_items.extend([vid.video_id for vid in already_checked])

            vid_ids = self.get_vid_ids(playlist_items, site)

            # Delete removed items from playlist
            self.update_removed_playlist_videos(playlist_row.id, list(vid_ids.values()))
            self.add_playlist_vids(playlist_row.id, vid_ids.values())

            # Update download cols
            if playlist.archive:
                self.update_archived_playlist_videos(playlist_row.id)

            if deleted:
                self.add_deleted_vids(deleted, site)

            # Add new tags
            self.add_vid_tags(items, site)

            # Cache channels
            all_channels = {v.channel_id for v in items}
            uncached = all_channels - self.channel_cache[site]
            cached_channels = all_channels - uncached
            del all_channels

            try:
                # Get channel data
                channels = playlist_checker.get_channels(uncached)
            except HttpError:
                pass
            else:
                # Only update channel infos if we successfully fetch them
                # Update channel cache
                self.channel_cache[site].update(channels)
                channels.extend(cached_channels)

                # Add channels and channel videos
                self.add_channel_videos(items, channels, site)

            checked_playlists.add(playlist_row.id)

            # After processing of data by external scripts
            after = playlist.after or []
            after.extend(self.config.after or [])  # Default after command

            if not after:
                logger.debug('No scripts to run after checking')

            if after:
                old = [d.video_id for d in old]
                new = items - {k for k, v in self.all_vids[site].items() if
                               v in old}

                # Get info of rest of the deleted vids
                deleted = self.get_deleted_info(deleted, site)

                logger.info(f'{len(new_deleted)} newly deleted videos')
                logger.info(f'{len(new)} new videos')

                fields = {
                    'url_format': playlist_checker.url_format,
                    'channel_format': playlist_checker.channel_url_format,
                    'playlist_format': playlist_checker.playlist_url_format,
                    'playlist_id': playlist_id,
                    'playlist_name': playlist_row.name or playlist.name
                }
                optional_fields = {
                    'deleted': [vid.to_dict() for vid in deleted],
                    'new_deleted': [vid.to_dict() for vid in new_deleted],
                    'new': [vid.to_dict() for vid in new]
                }

                thread = threading.Thread(target=self.run_after, args=(fields, optional_fields, after), daemon=True)
                thread.start()
                self.threads.append(thread)

            logger.info(f'Done checking {playlist_id}')

        after_all = self.config.after_all
        if after_all:
            # TODO do when it when you need it
            pass

        logger.info('Downloading videos')

        downloads = 0
        for row in self.iter_videos_to_download(playlist_ids=list(checked_playlists)):
            if 0 < self.config.max_downloads_per_run <= downloads:
                break

            site = row.site
            info = video_downloader.download_video(SITE_CLASSES[site](row.video_id),
                                                       row,
                                                       {},
                                                       self.config.download_sleep_interval)

            if info and info.success:
                self.update_vid_filename(info.filename, info.downloaded_format, row.id)
                downloads += 1

        logger.info('Videos downloaded')

        if self.threads:
            logger.debug('Waiting for threads to finish')
            timeout = 900/len(self.threads)
            for thread in self.threads:
                thread.join(timeout=timeout)

            if list(filter(lambda t: t.is_alive(), self.threads)):
                logger.warning('Threads open even after 15min. Force closing')
                exit()
