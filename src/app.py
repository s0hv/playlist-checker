import json
import logging
import os
import shlex
import subprocess
import threading
import time
from typing import List, TypeVar, Optional, cast

import psycopg
from psycopg.rows import dict_row

from src.api import YTApi, HttpError
from src.channel import BaseChannel, YTChannel
from src.config import Config, Script
from src.db import models
from src.db.dbutils import DbUtils
from src.downloaders import thumbnail, video as video_downloader
from src.enum import Site, S3ObjectType
from src.playlist import YTPlaylist
from src.utils import get_filename, json_serializer
from src.video import SITE_CLASSES, BaseVideo

logger = logging.getLogger('debug')
T = TypeVar('T')
BaseVideoT = TypeVar('BaseVideoT', bound=BaseVideo)


class PlaylistChecker:
    def __init__(self, config: Config = None):
        if not config:
            config = Config.load()

        self.config = config

        self.already_checked: dict[Site, set[BaseVideo]] = {site: set() for site in list(Site.__members__.values())}
        # Dict of dicts in the form {site: {video_id: db_id}}
        self.all_vids: dict[Site, dict[str, int]] = {site: {} for site in list(Site.__members__.values())}
        self.all_vid_ids: dict[Site, set[str]] = {site: set() for site in list(Site.__members__.values())}
        self.channel_cache: dict[Site, set[BaseChannel]] = {site: set() for site in list(Site.__members__.values())}
        self.db_channel_cache: dict[Site, set[str]] = {site: set() for site in list(Site.__members__.values())}

        self._yt_api = YTApi(self.config.yt_token)
        self.all_tags: dict[str, int] = {}
        self.threads: list[threading.Thread] = []

        self._conn = psycopg.connect(self.config.db_conn_string, row_factory=dict_row)
        self.db = DbUtils(self._conn)

    @property
    def conn(self) -> psycopg.Connection:
        if self._conn is None:
            raise ValueError('Connection was not initialized')

        return self._conn

    @property
    def yt_api(self):
        return self._yt_api

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
                p = subprocess.Popen(shlex.split(cmd), stdin=subprocess.PIPE, stderr=subprocess.PIPE)
            except FileNotFoundError:
                logger.exception('File "%s" not found' % cmd)
                continue

            p.stdin.write(
                json.dumps(data, ensure_ascii=False, default=json_serializer).encode('utf-8')
            )
            try:
                out, err = p.communicate()
            except:
                logger.exception(f'Failed to run script {after.name}')
            else:
                if out:
                    logger.info(f'Script output. {out.decode("utf-8")}')
                if err:
                    logger.error(f'Failed to run script {after.name}. {err.decode("utf-8")}')

    def download_videos(self, checked_playlists: list[int]) -> list[str]:
        downloads = 0
        delete_files = []
        for row in self.db.iter_videos_to_download(playlist_ids=checked_playlists):
            if 0 <= self.config.max_downloads_per_run <= downloads:
                break

            if not row.force_redownload and row.downloaded:
                continue

            logger.info(f'Downloading video {downloads+1} of max {self.config.max_downloads_per_run}')
            site = row.site
            info = video_downloader.download_video(SITE_CLASSES[site](row.video_id),
                                                   row,
                                                   {},
                                                   self.config.download_sleep_interval)

            if info.success:
                downloads += 1
                old_extras = None
                if self.config.delete_old_info or self.config.delete_old_thumbnail:
                    old_extras = self.db.get_extra_files(row.id)

                if not self.config.s3_archive:
                    self.db.update_vid_filename(info.filename, info.downloaded_format, row.id)
                    extra = models.VideoExtraFiles(
                        video_id=row.id,
                        thumbnail=info.thumbnail_path,
                        info_json=info.info_path,
                        subtitles=info.subtitle_paths
                    )
                    self.db.update_extra_files(extra)

                    if not old_extras:
                        continue

                    if self.config.delete_old_info and info.info_path:
                        self.delete_old_file(old_extras.info_json,
                                             info.info_path)

                    if self.config.delete_old_thumbnail and info.thumbnail_path:
                        self.delete_old_file(old_extras.thumbnail,
                                             info.thumbnail_path)
                    continue

                base_tags = {}

                s3_file = self.upload_and_delete_file(info.filename, base_tags, S3ObjectType.video)
                if s3_file:
                    self.db.update_vid_filename(s3_file, info.downloaded_format, row.id)

                    # Delete old file if force redownload
                    if row.force_redownload and row.downloaded_filename:
                        delete_files.append(row.downloaded_filename)
                else:
                    self.db.update_vid_filename(info.filename, info.downloaded_format, row.id)

                info_file = self.upload_and_delete_file(info.info_path, base_tags, S3ObjectType.metadata)
                thumbnail_file = self.upload_and_delete_file(info.thumbnail_path, base_tags, S3ObjectType.thumbnail)

                subs = []
                if info.subtitle_paths:
                    for sub in info.subtitle_paths:
                        sub_path = self.upload_and_delete_file(sub, base_tags, S3ObjectType.subtitle)
                        if sub_path is not None:
                            subs.append(sub_path)

                self.db.update_extra_files(models.VideoExtraFiles(
                    video_id=row.id,
                    thumbnail=thumbnail_file,
                    info_json=info_file,
                    subtitles=subs or None
                ))

                if old_extras:
                    # If old info json exists delete that
                    if self.config.delete_old_info and old_extras.info_json:
                        if info.info_path:
                            self.delete_old_file(old_extras.info_json, info.info_path)

                        # Make sure new thumbnail was uploaded and filename does not contain directories
                        if info_file and self.should_delete_s3(old_extras.info_json, info_file):
                            delete_files.append(old_extras.info_json)

                    # If old thumbnail exists delete that
                    if self.config.delete_old_thumbnail and old_extras.thumbnail:
                        if info.thumbnail_path:
                            self.delete_old_file(old_extras.thumbnail, info.thumbnail_path)

                        # Make sure new thumbnail was uploaded and filename does not contain directories
                        if thumbnail_file and self.should_delete_s3(old_extras.thumbnail, thumbnail_file):
                            delete_files.append(old_extras.thumbnail)

            else:
                downloads += 1
                if info.blocked:
                    self.db.update_vid_filename(None, None if row.downloaded_format else 'Video Unavailable', row.id)

        return delete_files

    def download_thumbnails(self, thumbnail_downloads: dict[Site, set[BaseVideo]]):
        for site, videos in thumbnail_downloads.items():
            if not videos:
                continue

            videos_dict: dict[str, BaseVideo] = {v.video_id: v for v in videos}

            # Must be called after all playlists have been processed for proper functionality
            partial_videos = self.db.get_thumbnails_to_dl(site)
            should_download = {}
            for vid in partial_videos:
                if found := videos_dict.get(vid.video_id):
                    should_download[vid.id] = found

            if not should_download:
                continue

            logger.info(f'Downloading {len(should_download)} thumbnails for site {Site(site).name}')
            thumbnail.bulk_download_thumbnails(should_download.values(), Site(site))

            # Update database with new filenames and upload to S3 if required
            for id_, vid in should_download.items():
                thumbnail_file = vid.thumbnail_path
                if not thumbnail_file:
                    continue

                if not self.config.s3_archive:
                    self.db.update_extra_files(models.VideoExtraFiles(
                        video_id=id_,
                        thumbnail=thumbnail_file
                    ))
                    continue

                base_tags = {}

                new_file = self.upload_and_delete_file(thumbnail_file, base_tags, S3ObjectType.thumbnail)
                if new_file:
                    thumbnail_file = new_file

                self.db.update_extra_files(models.VideoExtraFiles(
                    video_id=id_,
                    thumbnail=thumbnail_file
                ))

    def check_all(self, whitelist: list[str] = None, no_download: bool = False):
        """
        Main function of this class that runs the whole thing and
        does all the stuff to make everything work as intended

        Args:
            whitelist (list of str):
                Optional list of playlist ids if you want to only check
                specific playlists
        """
        logger.info('Starting check')
        with self.db.class_cursor(models.Playlist) as cur:
            sql = 'SELECT * FROM playlists'
            cur.execute(sql)
            _playlists: dict[str, models.Playlist] = {data.playlist_id: data for data in cur}

        with self.db.class_cursor(models.PartialVideo) as cur:
            sql = 'SELECT id, video_id, site FROM videos'
            cur.execute(sql)

            for vid in cur:
                self.all_vids[Site(vid.site)][vid.video_id] = vid.id
                self.all_vid_ids[Site(vid.site)].add(vid.video_id)

        with self.db.class_cursor(models.Tag) as cur:
            # Put all existing tags to cache
            sql = 'SELECT * FROM tags'
            cur.execute(sql)
            for tag in cur:
                self.all_tags[tag.tag] = tag.id

        with self.db.class_cursor(models.Channel) as cur:
            # Put all inserted channel ids to cache
            sql = 'SELECT id, site, channel_id, name FROM channels'
            cur.execute(sql)
            for channel in cur:
                self.db_channel_cache[channel.site].add(channel.channel_id)

        playlists = self.config.playlists
        logger.info(f'Checking a total of {len(playlists)} playlists')
        checked_playlists: set[int] = set()
        thumbnail_downloads: dict[Site, set[BaseVideo]] = {site: set() for site in list(Site.__members__.values())}

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
            if site == Site.YouTube:
                playlist_checker = YTPlaylist(self.conn, self.yt_api, playlist_id)
                if not playlist_row:
                    logger.info('New playlist getting playlist info')
                    info = playlist_checker.get_playlist_info()
                    if not info:
                        continue

                    playlist_row = self.db.add_playlist(playlist_id, info['snippet']['title'], site)
            else:
                logger.warning(f'{site} not implemented')
                continue

            # Get videos
            logger.debug('getting old ids')
            old = self.db.get_playlist_video_ids(playlist_row.id)
            logger.debug('Getting items from youtube')

            try:
                # Items contains undeleted videos
                retval = playlist_checker.get_videos(self.already_checked[site])
                if retval is None:
                    continue

                items, deleted, already_checked = retval
            except HttpError:
                # Skip on playlist http error
                continue

            thumbnail_downloads[site].update(items)

            # Get new deleted videos
            new_deleted = self.db.get_new_deleted(deleted, site)

            # Update video cache
            self.already_checked[site].update(items)
            self.already_checked[site].update(deleted)

            # Add new vids to db and update old items
            logger.debug('Adding and updating vids')
            self.db.add_and_update_vids(items, self.all_vid_ids[site], site)

            # Put all vids in the playlist to a single list
            # in order get the db ids, so we can update
            # the playlistVideos table correctly
            playlist_items = [item.video_id for item in items]
            playlist_items.extend([vid.video_id for vid in deleted])
            playlist_items.extend([vid.video_id for vid in already_checked])

            vid_ids = self.db.get_vid_ids(playlist_items, site)
            self.all_vids[site].update(vid_ids)
            self.all_vid_ids[site].update(vid_ids.keys())

            # Delete removed items from playlist
            self.db.update_removed_playlist_videos(playlist_row.id, list(vid_ids.values()))
            self.db.add_playlist_vids(playlist_row.id, vid_ids.values())

            # Update download cols
            if playlist.archive:
                self.db.update_archived_playlist_videos(playlist_row.id)

            if deleted:
                self.db.add_deleted_vids(deleted, self.all_vid_ids[site], site)

            # Add new tags
            self.db.add_vid_tags(items, self.all_tags, self.all_vids[site])

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
                channels.extend(cast(set[YTChannel], cached_channels))

                # Add channels and channel videos
                self.db.add_channel_videos(items, channels, self.db_channel_cache[site], self.all_vids[site], site)

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
                deleted_to_script: list[models.VideoToScript] = self.db.get_deleted_info(deleted, site)
                new_deleted_to_script: list[models.VideoToScript] = self.db.get_deleted_info(new_deleted, site)

                logger.info(f'{len(new_deleted)} newly deleted videos')
                logger.info(f'{len(new)} new videos')

                fields = {
                    'url_format': playlist_checker.url_format,
                    'channel_format': playlist_checker.channel_url_format,
                    'playlist_format': playlist_checker.playlist_url_format,
                    'playlist_id': playlist_id,
                    'playlist_name': playlist_row.name or playlist.name,
                    'filename_template': self.config.filename_template
                }
                optional_fields = {
                    'deleted': [vid.__dict__ for vid in deleted_to_script],
                    'new_deleted': [vid.__dict__ for vid in new_deleted_to_script],
                    'new': [vid.__dict__ for vid in self.db.videos_for_script(new, site)]
                }

                thread = threading.Thread(target=self.run_after, args=(fields, optional_fields, after), daemon=True)
                thread.start()
                self.threads.append(thread)

            logger.info(f'Done checking {playlist_id}')

        after_all = self.config.after_all
        if after_all:
            # TODO do when it when you need it
            pass

        if not no_download:
            logger.info(f'Downloading videos from playlists {checked_playlists}')

            delete_files = self.download_videos(list(checked_playlists))
            logger.info('Videos downloaded')

            self.download_thumbnails(thumbnail_downloads)

            if self.config.s3_archive:
                from src.s3 import upload
                upload.delete_files(delete_files, self.config.s3_bucket)

        if self.threads:
            logger.debug('Waiting for threads to finish')
            timeout = 900
            current_time = time.perf_counter()

            while (time.perf_counter() - current_time) < timeout:
                for thread in self.threads:
                    if not thread.is_alive():
                        thread.join(timeout=1)

                # Check if all threads finished
                if all(not t.is_alive() for t in self.threads):
                    break

                time.sleep(5)

            if threads := list(filter(lambda t: t.is_alive(), self.threads)):
                logger.error(f'{len(threads)} threads open even after 15min. Force closing {threads}')
                self.conn.commit()
                exit()

    @staticmethod
    def should_delete_s3(old_path: Optional[str], new_name: str) -> bool:
        if not PlaylistChecker.should_delete(old_path, new_name):
            return False

        # Make sure old file was most likely uploaded to S3 (doesn't contain a folder in the name)
        return get_filename(old_path) == old_path

    @staticmethod
    def should_delete(old_path: Optional[str], new_path: str) -> bool:
        if old_path is None or old_path == new_path:
            return False

        return True

    def delete_old_file(self, old_path: Optional[str], new_path: str):
        if not self.should_delete(old_path, new_path):
            return

        if not os.path.exists(old_path):
            return

        logger.info(f'Deleting old file since a new file was downloaded. {old_path} -> {new_path}')
        try:
            os.remove(old_path)
        except OSError:
            logger.exception(f'Failed to remove file {old_path}')

    def upload_and_delete_file(self, filename: Optional[str], base_tags: dict, object_type: S3ObjectType) -> Optional[str]:
        if not filename:
            return

        from src.s3.upload import upload_file

        logger.debug(f'Uploading {filename} with type {object_type}')
        s3_filename = upload_file(filename, self.config.s3_bucket, {
            **base_tags,
            'type': object_type.value
        })

        if not s3_filename:
            return

        try:
            os.remove(filename)
        except OSError:
            logger.exception(f'Failed to remove file {filename}')

        return s3_filename
