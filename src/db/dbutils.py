import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Callable, Concatenate, TypeVar, ParamSpec, Iterable, Type, \
    cast, Sequence, Optional, Generator

from psycopg import Cursor, Connection
from psycopg.rows import class_row, BaseRowFactory
from psycopg.types.json import Json

from src.channel import BaseChannel
from src.db import models
from src.db.utils import execute_values
from src.enum import Site
from src.video import BaseVideo

logger = logging.getLogger('debug')

CursorT = TypeVar('CursorT', bound=Cursor)
ConnectionT = TypeVar('ConnectionT', bound=Connection)
BaseVideoT = TypeVar('BaseVideoT', bound=BaseVideo)


P = ParamSpec('P')
T = TypeVar('T')


def transaction(row_factory: BaseRowFactory[T] = None, real_transaction: bool = False, no_cursor: bool = False):
    def _transaction(f: Callable[Concatenate['WithConnection', P], T]) -> Callable[Concatenate['WithConnection', P], T]:
        def wrapper(self: WithConnection, *args: P.args, **kwargs: P.kwargs) -> T:
            def run_fn() -> T:
                if not no_cursor and 'cur' not in kwargs:
                    with self.conn.cursor(row_factory=row_factory) as cur:
                        kwargs['cur'] = cur
                        return f(self, *args, **kwargs)

                return f(self, *args, **kwargs)

            if real_transaction:
                with self.conn.transaction():
                    return run_fn()

            try:
                retval = run_fn()
                self.conn.commit()
                return retval
            except Exception:
                self.conn.rollback()
                raise

        return wrapper

    return _transaction


class WithConnection:
    def __init__(self, conn: ConnectionT):
        self._conn = conn

    @property
    def conn(self) -> ConnectionT:
        return self._conn

    @contextmanager
    def class_cursor(self, cls: Type[T]) -> Cursor[T]:
        with self.conn.cursor(row_factory=class_row(cls)) as cur:
            yield cur


class DbUtils(WithConnection):
    @transaction(row_factory=class_row(models.Playlist))
    def add_playlist(self, playlist_id: str, name: str, site: int | Site,
                     cur: Cursor[models.Playlist] = NotImplemented) -> models.Playlist:
        """
        Adds a playlist to the database if it doesn't exist

        Args:
            playlist_id:
                id of the playlist
            name:
                name of the playlist
            site:
                ID of the site being used
            cur:
                Optional cursor

        Returns:
            int: The database id of the newly made playlist

        """
        site = int(site)
        sql = 'INSERT INTO playlists (playlist_id, name, site) VALUES (%s, %s, %s) RETURNING *'
        cur.execute(sql, (playlist_id, name, site))
        return cur.fetchone()

    @transaction()
    def update_archived_playlist_videos(self, playlist_id: int, cur: Cursor = NotImplemented):
        """
        Use when archive property is set to true on a playlist.
        Will set the download flag to true on each video in the playlist
        """
        sql = '''
        UPDATE videos v SET download=TRUE
        FROM playlistvideos pv
        WHERE pv.playlist_id=%s AND v.id=pv.video_id AND v.download=FALSE
        '''
        cur.execute(sql, (playlist_id,))
        logger.info(f'Set download col for {cur.rowcount} videos')

    @transaction(real_transaction=True)
    def add_and_update_vids(self, videos: Iterable[BaseVideo], existing_ids: set[str], site: int | Site, cur: Cursor = NotImplemented):
        """
        Adds new videos to database and updates the following properties from
        duplicate entries:
            title, description, thumbnail url, publish date
            also sets the deleted flag to False

        Do not give deleted videos to this function. It will set the deleted
        flag to true on them

        Args:
            videos:
                Iterable of :class:BaseVideo that will be added or updated
            existing_ids:
                Set of all video ids that are in the database for the given site.
            site:
                id of the site being used
            cur:
                Optional cursor
        """
        videos_set = set(videos)
        # We can't use intersection because it wouldn't give our custom object
        # instead it always would give a set of values used for comparison
        do_insert = videos_set - existing_ids
        do_update = videos_set - do_insert

        if not (do_insert or do_update):
            return

        site = int(site)

        if do_insert:
            sql = 'INSERT INTO videos AS v (video_id, title, description, published_at, site, thumbnail) ' \
                  'VALUES %s'

            values = tuple(
                (vid.video_id, vid.title, vid.description, vid.published_at, site, vid.thumbnail)
                for vid in do_insert
            )

            execute_values(cur, sql, values, page_size=500)

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

            execute_values(cur, sql, values, page_size=500)

    @transaction(real_transaction=True)
    def add_deleted_vids(self, videos: Iterable[BaseVideo], existing_ids: set[str], site: int | Site, cur: Cursor = NotImplemented):
        """
        Sets the deleted flag on the videos provided and also sets the
        deletion time column if the deleted flag hasn't been set before

        Args:
            videos:
                Iterable of BaseVideo that are deleted
            site:
                id of the site being used
            cur:
                Optional cursor
        """
        videos = set(videos)
        do_insert = videos - existing_ids
        do_update = videos - do_insert

        if not (do_insert or do_update):
            return

        site = int(site)
        if do_insert:
            t = datetime.utcnow()
            sql = 'INSERT INTO videos (video_id, title, published_at, site, deleted, deleted_at) VALUES %s'

            values = tuple((vid.video_id, t, t) for vid in do_insert)

            execute_values(cur, sql, values, page_size=1000, template=f"(%s, 'Deleted video', %s, {site}, True, %s)")

        if do_update:
            sql = 'UPDATE videos AS v SET ' \
                  'deleted_at=CASE WHEN v.deleted=FALSE THEN CURRENT_TIMESTAMP ELSE v.deleted_at END, ' \
                  'deleted=TRUE ' \
                  'WHERE site=%s AND video_id=ANY(%s)'

            cur.execute(sql, [site, [v.video_id for v in do_update]])

    @transaction(no_cursor=True, real_transaction=True)
    def add_vid_tags(self,
                     videos: set[BaseVideo],
                     cached_tags: dict[str, int],
                     all_videos: dict[str, int],
                     default_tags: list[str] = None):
        """
        Adds missing tags to the database based on the provided videos.
        Also updates cached_tags and all_videos

        Args:
            videos:
                List of videos from which the tags will be added
            cached_tags:
                tags that have already been cached to memory
            all_videos:
                All videos stored in the db of the specified site.
            default_tags:
                An list of tag names to be applied to every video in the videos
                param
        """
        default_tags = [] if not default_tags else default_tags
        tag_values: set[str] = set(default_tags)
        cached_tag_names = set(cached_tags.keys())

        # Copy the list of videos since we don't want to edit the original list
        videos = videos.copy()
        for vid in videos.copy():  # This copy is probably needed
            if not vid.data:
                videos.remove(vid)
                continue

            tags = vid.tags
            if not tags:
                videos.remove(vid)
                continue

            tag_values.update(map(str.lower, tags))

        # Get non cached tags and add them to db
        tobecached = tag_values - cached_tag_names

        if tobecached:
            sql = 'INSERT INTO tags (tag) VALUES %s ON CONFLICT DO NOTHING RETURNING tag, id'
            with self.class_cursor(models.Tag) as cursor:
                results: list[models.Tag] = execute_values(cursor, sql,
                                                           [(x,) for x in tobecached],
                                                           page_size=1000, fetch=True)

            for tag in results:
                cached_tags[tag.tag] = tag.id

        values = []
        for vid in videos:
            video_id = all_videos.get(vid.video_id)
            if not video_id:
                logger.warning('Video id not found with %s' % vid)
                continue

            # Add video specific tags
            for tag in vid.tags:
                tag_id = cached_tags.get(tag.lower())
                if not tag_id:
                    logger.warning('Tag %s not found' % tag)
                    continue

                values.append((tag_id, video_id))

            # Add default tags
            for default_tag in default_tags:
                tag_id = cached_tags.get(default_tag.lower())
                if not tag_id:
                    logger.warning('Tag %s not found' % default_tag)
                    continue

                values.append((tag_id, video_id))

        sql = 'INSERT INTO videoTags (tag_id, video_id) VALUES %s ON CONFLICT DO NOTHING '

        with self.conn.cursor() as cursor:
            execute_values(cursor, sql, values, page_size=2000)

    @transaction(real_transaction=True)
    def add_channels(self, channels: Iterable[BaseChannel], channel_cache: set[str], site: int | Site, cur: Cursor = NotImplemented):
        """
        Adds channels to db and updates old entries
        Columns updated are as follows:
            name and thumbnail aka profile pic

        Args:
            channels: iterable of channels to add
            channel_cache: Set of all channels ids in db
            site: id of the site
            cur: Optional cursor

        """
        site = int(site)
        channels = set(channels)
        do_insert = channels - cast(set[BaseChannel], channel_cache)
        do_update = channels - do_insert

        if not (do_insert or do_update):
            return

        if do_insert:
            sql = 'INSERT INTO channels (channel_id, name, thumbnail, site) VALUES %s'

            execute_values(cur, sql, [(c.channel_id, c.name, c.thumbnail, site) for c in do_insert], page_size=1000)

            channel_cache.update([c.channel_id for c in do_insert])

        if do_update:
            sql = 'UPDATE channels AS c SET ' \
                  'name=COALESCE(v.name, c.name), ' \
                  'thumbnail=COALESCE(v.thumbnail, c.thumbnail) ' \
                  'FROM (VALUES %s) AS v(channel_id, name, thumbnail) ' \
                  'WHERE v.channel_id=c.channel_id'

            execute_values(cur, sql, [(c.channel_id, c.name, c.thumbnail) for c in do_update], page_size=1000)

    @transaction(no_cursor=True)
    def add_channel_videos(self,
                           videos: Iterable[BaseVideo],
                           channels: Sequence[BaseChannel | str],
                           channel_cache: set[str],
                           db_videos:  dict[str, int],
                           site: int | Site):
        """
        Link video ids to channel ids in the channelVideos table
        This will handle adding missing channels for you. The videos need
        to have the channel property set to for this to work

        Args:
            videos: List of :class:BaseVideo instances
            channels: List of BaseChannel instances and channel_ids as str
            channel_cache: List of cached channel ids
            db_videos: video_id to db id
            site: id of the site being used
        """
        self.add_channels([c for c in channels if not isinstance(c, str)], channel_cache, site)
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

            vid_id = db_videos.get(vid.video_id)
            if not vid_id:
                continue

            data.append((channel_id, vid_id))

        sql = 'INSERT INTO channelVideos (channel_id, video_id) VALUES %s ON CONFLICT DO NOTHING'
        with self.conn.cursor() as cursor:
            execute_values(cursor, sql, data, page_size=2000)

    @transaction()
    def add_playlist_vids(self, playlist_id: int, video_ids: Iterable[int], cur: Cursor = NotImplemented):
        """
        Add video playlist connection to the playlistVideos table

        Args:
            playlist_id:
                The database id for the playlist
            video_ids:
                An iterable of database ids for videos that are added the
                specified playlist
            cur: optional cursor
        """
        sql = 'INSERT INTO playlistVideos (playlist_id, video_id) VALUES ' \
              '%s ON CONFLICT DO NOTHING'

        values = tuple((playlist_id, video_id) for video_id in video_ids)

        execute_values(cur, sql, values, page_size=2000)

    @transaction()
    def update_removed_playlist_videos(self, playlist_id: int, video_ids: list[int], cur: Cursor = NotImplemented):
        """
        Removes playlist videos that are not found in the video_ids iterable.
        """
        sql = 'DELETE FROM playlistvideos WHERE playlist_id=%s AND NOT video_id=ANY(%s)'
        cur.execute(sql, [playlist_id, video_ids])
        logger.info(f'User removed {cur.rowcount} videos from playlist {playlist_id}')

    @transaction(row_factory=class_row(models.PartialVideo))
    def get_vid_ids(self, vid_ids: list[str], site: int | Site, cur: Cursor[models.PartialVideo] = NotImplemented) -> dict[str, int]:
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

        cur.execute(sql, [vid_ids])
        vid_ids = {vid.video_id: vid.id for vid in cur}

        return vid_ids

    @transaction(row_factory=class_row(models.PlaylistVideo))
    def get_playlist_video_ids(self, playlist_id: int, cur: Cursor[models.PlaylistVideo] = NotImplemented) -> list[models.PlaylistVideo]:
        """
        Gets all video ids that are associated with this playlist
        Args:
            playlist_id: id of the playlist
            cur: Optional cursor
        Returns:
            list:
                A list PlaylistVideo objects with the video_id property set
        """
        sql = 'SELECT video_id FROM playlistVideos WHERE playlist_id=%s'
        cur.execute(sql, (playlist_id,))
        return cur.fetchall()

    @transaction(row_factory=class_row(models.VideoExtraFiles))
    def get_extra_files(self, video_id: int, cur: Cursor[models.VideoExtraFiles] = NotImplemented) -> Optional[models.VideoExtraFiles]:
        sql = 'SELECT * FROM extra_video_files WHERE video_id=%s'
        cur.execute(sql, (video_id,))
        return cur.fetchone()

    @transaction(row_factory=class_row(models.PartialVideo))
    def get_thumbnails_to_dl(self, site: int | Site, cur: Cursor[models.PartialVideo] = NotImplemented) -> list[models.PartialVideo]:
        """
        Finds videos without thumbnail set in extra files
        """
        sql = '''
        SELECT v.video_id, v.id FROM videos v
        LEFT JOIN extra_video_files evf ON v.id = evf.video_id
        WHERE v.site=%s AND v.deleted=FALSE AND evf.thumbnail IS NULL AND 
            (v.download=FALSE OR (v.force_redownload=FALSE AND v.downloaded_format IS NOT NULL))
        '''
        cur.execute(sql, (int(site),))
        return cur.fetchall()

    def iter_videos_to_download(self, playlist_ids: list[int] = None) -> Generator[models.Video, None, None]:
        where = '((download=TRUE or force_redownload=TRUE) AND deleted=FALSE)'
        join = ''
        args = ()

        if playlist_ids:
            where = 'pv.playlist_id=ANY(%s) AND ' + where
            join = 'INNER JOIN playlistvideos pv ON v.id = pv.video_id'
            args = [playlist_ids]

        sql = f'''
        SELECT DISTINCT ON (id) id, site, v.video_id, downloaded_format, downloaded_filename, download_format, force_redownload
        FROM videos v
        {join}
        WHERE {where}
        '''

        with self.class_cursor(models.Video) as cursor:
            cursor.execute(sql, args)
            for row in cursor:
                yield row

    @transaction()
    def update_vid_filename(self, filename: Optional[str], downloaded_format: Optional[str], video_id: int, cur: Cursor = NotImplemented):
        sql = '''
        UPDATE videos SET 
            downloaded_filename=COALESCE(%s, downloaded_filename), 
            downloaded_format=COALESCE(%s, downloaded_format), 
            force_redownload=FALSE
        WHERE id=%s
        '''
        cur.execute(sql, (filename, downloaded_format, video_id))

    @transaction()
    def update_filename(self, filename: str, video_id: int, cur: Cursor = NotImplemented):
        cur.execute('UPDATE videos SET downloaded_filename=%s WHERE id=%s', (filename, video_id))

    @transaction()
    def update_extra_files(self, model: models.VideoExtraFiles, cur: Cursor = NotImplemented):
        sql = '''
        INSERT INTO extra_video_files as e (video_id, thumbnail, info_json, other_files, audio_file) 
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (video_id) DO UPDATE 
        SET thumbnail=COALESCE(EXCLUDED.thumbnail, e.thumbnail), 
            info_json=COALESCE(EXCLUDED.info_json, e.info_json), 
            other_files=COALESCE(EXCLUDED.other_files, e.other_files),
            audio_file=COALESCE(EXCLUDED.audio_file, e.audio_file)
        '''
        other_files = Json(model.other_files) if model.other_files else None
        cur.execute(sql, (model.video_id, model.thumbnail, model.info_json, other_files, model.audio_file))

    @transaction()
    def get_deleted_info(self, deleted: set[BaseVideoT], site: int | Site, cur: Cursor = NotImplemented) -> set[BaseVideoT]:
        """
        Updates BaseVideo objects with cached info from database
        Namely updates title, channel name and channel id

        Args:
            deleted:
                 List of the videos to be updated
            site:
                Id of the site used
            cur: optional cursor

        Returns:
            list: Exactly the same list as it was given

        """
        if not deleted:
            return deleted

        site = int(site)
        sql = 'SELECT v.video_id, v.title, c.name, c.channel_id FROM videos v INNER JOIN channelVideos cv ' \
              'ON cv.video_id=v.id INNER JOIN channels c ON cv.channel_id = c.id ' \
             f'WHERE v.site=%s AND v.video_id=ANY(%s)'

        cur.execute(sql, [site, [vid.video_id for vid in deleted]])

        for row in cur:
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

    @transaction(row_factory=class_row(models.Video))
    def get_new_deleted(self, deleted: set[BaseVideoT], site: int | Site,
                        cur: Cursor[models.Video] = NotImplemented) -> set[BaseVideoT]:
        """
        Gets the newly deleted videos from the specified site with
        updated titles

        Args:
            deleted:
                List of all deleted vids from a site.
            site:
                id if the site currently in use
            cur: optional cursor

        Returns:
            set: A set of BaseVideo objects with updated titles
        """
        if not deleted:
            return set()

        site = int(site)
        sql = f'SELECT id, title, video_id FROM videos WHERE deleted IS FALSE AND site=%s' \
               ' AND video_id=ANY(%s)'

        new_deleted = set()
        cur.execute(sql, [site, [vid.video_id for vid in deleted]])

        for video_partial in cur:
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
