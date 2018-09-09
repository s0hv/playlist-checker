from src.enums import Sites
import pymysql.cursors
from src.api import YTApi
from src.playlist import YTPlaylist
from src.video import YTVideo
from src.channel import YTChannel
from datetime import datetime
import subprocess
import shlex
import json
import threading
import logging

logger = logging.getLogger('debug')


class PlaylistChecker:
    def __init__(self, config):
        self.config = config
        self.already_checked = {site: set() for site in list(Sites.__members__.values())}
        self.all_vids = {site: {} for site in list(Sites.__members__.values())}
        self.channel_cache = {site: set() for site in list(Sites.__members__.values())}

        self._db = pymysql.connect(host=self.config['db_host'],
                                   port=self.config['db_port'],
                                   user=self.config['db_user'],
                                   password=self.config['db_pass'],
                                   db=self.config['db'],
                                   charset='utf8mb4',
                                   cursorclass=pymysql.cursors.DictCursor)

        self._yt_api = YTApi(self.config['yt_api'])
        self.all_tags = {}
        self.threads = []

    @staticmethod
    def datetime2sql(datetime):
        return '{0.year}-{0.month}-{0.day} {0.hour}:{0.minute}:{0.second}'.format(datetime)

    @property
    def db(self):
        return self._db

    @property
    def yt_api(self):
        return self._yt_api

    def add_and_update_vids(self, videos, site):
        sql = 'INSERT INTO `videos` (`video_id`, `title`, `description`, `published_at`, `site`, `thumbnail`) VALUES ' \
              f'(%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE title=IF(`title`!=VALUES(`title`), VALUES(`title`), `title`), ' \
              'description=IF(`description`!=VALUES(`description`), VALUES(`description`), `description`), deleted=FALSE,' \
              'thumbnail=IF(VALUES(`thumbnail`) IS NULL, thumbnail, VALUES(`thumbnail`)), ' \
              'published_at=VALUES(`published_at`)'

        values = ((vid.video_id, vid.title, vid.description, self.datetime2sql(vid.published_at), site, vid.thumbnail)
                  for vid in videos)

        with self.db.cursor() as cursor:
            cursor.executemany(sql, values)

        self.db.commit()

    def add_deleted_vids(self, videos, site):
        t = self.datetime2sql(datetime.utcnow())
        sql = 'INSERT INTO `videos` (`video_id`, `title`, `published_at`, `site`, `deleted`, `deleted_at`) VALUES ' \
              f'(%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE deleted_at=IF(`deleted`=FALSE, CURRENT_TIMESTAMP, `deleted_at`), deleted=TRUE'

        values = ((vid.video_id, "Deleted video", t, site, True, t) for vid in videos)

        with self.db.cursor() as cursor:
            cursor.executemany(sql, values)

        self.db.commit()

    def add_vid_tags(self, videos, site, default_tags=None):
        sql = 'INSERT IGNORE INTO `tags` (`tag`) VALUES (%s)'
        default_tags = [] if not default_tags else default_tags
        values = set(default_tags)
        cached_tags = set(self.all_tags.keys())

        for vid in videos.copy():
            if not vid.data:
                videos.remove(vid)
                continue

            tags = vid.tags
            if not tags:
                videos.remove(vid)
                continue

            values.update(map(str.lower, tags))

        with self.db.cursor() as cursor:
            cursor.executemany(sql, list(values))

        self.db.commit()

        tobecached = values - cached_tags

        format_tags = ','.join(['%s'] * len(tobecached))
        sql = 'SELECT * FROM `tags` WHERE `tag` IN (%s)' % format_tags

        with self.db.cursor() as cursor:
            cursor.execute(sql, list(tobecached))
            for tag in cursor:
                self.all_tags[tag['tag']] = tag['id']

        values = []
        for vid in videos:
            for tag in vid.tags:
                tag_id = self.all_tags.get(tag.lower())
                if not tag_id:
                    print('Tag %s not found' % tag)
                    continue

                video_id = self.all_vids[site].get(vid.video_id)
                if not video_id:
                    print('Video id not found with %s' % vid)
                    continue

                values.append((tag_id, video_id))

        sql = 'INSERT IGNORE INTO videoTags (`tag_id`, `video_id`) VALUES (%s, %s)'

        with self.db.cursor() as cursor:
            cursor.executemany(sql, values)

        self.db.commit()

    def add_channels(self, channels):
        sql = 'INSERT IGNORE INTO `channels` (`channel_id`, `name`, `thumbnail`) VALUES (%s, %s, %s) ' \
              'ON DUPLICATE KEY UPDATE name=IF(VALUES(`name`) IS NULL, name, VALUES(`name`)), ' \
              'thumbnail=IF(VALUES(`thumbnail`) IS NULL, thumbnail, VALUES(`thumbnail`))'

        with self.db.cursor() as cursor:
            cursor.executemany(sql, [(c.channel_id, c.name, c.thumbnail) for c in channels])

        self.db.commit()

    def add_channel_videos(self, videos, channels, site):
        self.add_channels([c for c in channels if not isinstance(c, str)])

        format_channels = ','.join(['%s'] * len(channels))
        sql = 'SELECT id, channel_id FROM `channels` WHERE channel_id IN (%s)' % format_channels

        channel_ids = {}
        with self.db.cursor() as cursor:
            cursor.execute(sql, [c if isinstance(c, str) else c.channel_id for c in channels])

            for row in cursor:
                channel_ids[row['channel_id']] = row['id']

        sql = 'INSERT IGNORE INTO `channelVideos` (`channel_id`, `video_id`) VALUES (%s, %s)'

        data = []

        for vid in videos:
            channel_id = channel_ids.get(vid.channel_id)
            if not channel_id:
                continue

            vid_id = self.all_vids[site].get(vid.video_id)
            if not vid_id:
                continue

            data.append((channel_id, vid_id))

        with self.db.cursor() as cursor:
            cursor.executemany(sql, data)

        self.db.commit()

    def add_playlist_vids(self, playlist_id, video_ids):
        sql = 'INSERT IGNORE INTO `playlistVideos` (`playlist_id`, `video_id`) VALUES ' \
              '(%s, %s)'

        values = ((playlist_id, video_id) for video_id in video_ids)

        with self.db.cursor() as cursor:
            cursor.executemany(sql, values)

        self.db.commit()

    def get_vid_ids(self, vid_ids, site):
        format_ids = ','.join(['%s'] * len(vid_ids))
        sql = f'SELECT id, video_id FROM `videos` WHERE site={site} AND video_id IN (%s)' % format_ids

        with self.db.cursor() as cursor:
            cursor.execute(sql, vid_ids)
            vid_ids = {vid['video_id']: vid['id'] for vid in cursor}

        self.all_vids[site].update(vid_ids)
        return vid_ids

    def add_playlist(self, playlist_id, name, site):
        sql = 'INSERT INTO `playlists` (`playlist_id`, `name`, `site`) VALUES (%s, %s, %s)'

        with self.db.cursor() as cursor:
            cursor.execute(sql, (playlist_id, name, site))
            playlist_id = cursor.lastrowid

        self.db.commit()

        return playlist_id

    def get_playlist_video_ids(self, playlist_id: int):
        sql = 'SELECT video_id FROM `playlistVideos` WHERE playlist_id=%s' % playlist_id

        with self.db.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()

    @staticmethod
    def run_after(data, cmds):
        for after in cmds:
            try:
                p = subprocess.Popen(shlex.split(after), stdin=subprocess.PIPE)
            except FileNotFoundError:
                logger.exception('File "%s" not found' % after)
                continue

            p.stdin.write(data.encode('utf-8'))
            try:
                p.communicate()
            except:
                logger.exception('Failed to run script %s' % after)

    def get_new_deleted(self, deleted, site):
        if not deleted:
            return []

        deleted_format = ','.join(['%s']*len(deleted))
        sql = f'SELECT title, video_id FROM `videos` WHERE deleted IS FALSE AND site={site}' \
               ' AND video_id IN (%s)' % deleted_format

        new_deleted = set()
        with self.db.cursor() as cursor:
            cursor.execute(sql, [vid.video_id for vid in deleted])

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
                new_deleted.add(video)

        return new_deleted

    def get_deleted_info(self, deleted, site):
        if not deleted:
            return ()

        deleted_format = ','.join(['%s']*len(deleted))
        sql = 'SELECT v.video_id, v.title, c.name, c.channel_id FROM `videos` v INNER JOIN `channelVideos` cv ' \
              'ON cv.video_id=v.id INNER JOIN `channels` c ON cv.channel_id = c.id ' \
             f'WHERE site={site} AND v.video_id IN (%s)' % deleted_format

        with self.db.cursor() as cursor:
            cursor.execute(sql, [vid.video_id for vid in deleted])

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

    def check_all(self):
        logger.info('Starting check')
        with self.db.cursor() as cursor:
            sql = 'SELECT * FROM `playlists`'
            cursor.execute(sql)
            _playlists = {data['playlist_id']: data for data in cursor.fetchall()}

            sql = 'SELECT id, video_id, site FROM `videos`'
            cursor.execute(sql)

            for vid in cursor:
                self.all_vids[vid['site']][vid['video_id']] = vid['id']

        playlists = self.config['playlists']
        logger.info(f'Checking a total of {len(playlists)} playlists')
        for playlist in playlists:
            playlist_id = playlist['playlist_id']
            playlist_data = _playlists.get(playlist_id, {})
            site = playlist['site']
            logger.info(f'Checking playlist {playlist_id} on site {site}')

            # Create playlist by site
            if site == Sites.Youtube:
                playlist_checker = YTPlaylist(self.db, self.yt_api, playlist_id)
                if not playlist_data:
                    logger.info('New playlist getting playlist info')
                    info = playlist_checker.get_playlist_info()
                    if not info:
                        continue

                    playlist_data['id'] = self.add_playlist(playlist_id, info['snippet']['title'], site)
                    playlist_data['name'] = info['snippet']['title']

                # Get videos
                old = self.get_playlist_video_ids(playlist_data['id'])
                items, deleted, already_checked = playlist_checker.get_videos(self.already_checked[site])

                # Get new deleted videos
                new_deleted = self.get_new_deleted(deleted, site)

                # Update video cache
                self.already_checked[site].update(items)
                self.already_checked[site].update(deleted)

                # Add new vids to db and update old items
                self.add_and_update_vids(items, site)

                # Put all vids in the playlist to a single list
                # in order get the db ids so we can update
                # the playlistVideos table correctly
                playlist_items = [item.video_id for item in items]
                playlist_items.extend([vid.video_id for vid in deleted])
                playlist_items.extend([vid.video_id for vid in already_checked])

                vid_ids = self.get_vid_ids(playlist_items, site)
                self.add_playlist_vids(playlist_data['id'], vid_ids.values())
                if deleted:
                    self.add_deleted_vids(deleted, site)

                # Add new tags
                self.add_vid_tags(items, site)

                # Cache channels
                cached_channels = {v.channel_id for v in items}
                channels = cached_channels - self.channel_cache[site]
                cached_channels = cached_channels - channels
                channels = playlist_checker.get_channels(channels)
                self.channel_cache[site].update(channels)
                channels.extend(cached_channels)

                # Add channels and channel videos
                self.add_channel_videos(items, channels, site)

                # After processing of data by external scripts
                after = playlist.get('after', [])
                after.extend(self.config.get('after', []))  # Default after command

                if after:
                    old = [d['video_id'] for d in old]
                    new = items - {k for k, v in self.all_vids[site].items() if
                                   v in old}

                    # Get info of rest of the deleted vids
                    deleted = self.get_deleted_info(deleted, site)

                    # Base json format
                    d = {'url_format': playlist_checker.url_format,
                         'channel_format': playlist_checker.channel_url_format,
                         'playlist_format': playlist_checker.playlist_url_format,
                         'playlist_id': playlist_id,
                         'playlist_name': playlist_data.get('name', playlist.get('name', 'Unnamed'))}

                    # We don't want to create the lists for nothing so we call
                    # the generators on demand
                    add_deleted = lambda: [vid.to_dict() for vid in deleted]
                    add_new_deleted = lambda: [vid.to_dict() for vid in new_deleted]
                    add_new = lambda: [vid.to_dict() for vid in new]

                    fields = {'deleted': add_new_deleted, 'new_deleted': add_new_deleted,
                              'new': add_new}

                    # Get all the fields the scripts require
                    required_fields = set()
                    required_fields.update(playlist.get('required_fields', []))
                    required_fields.update(self.config.get('required_fields', []))
                    if required_fields:
                        for field in required_fields:
                            f = fields.get(field)
                            if not f:
                                continue

                            d[field] = f()
                    else:
                        d.update({'deleted': add_deleted,
                                  'new_deleted': add_new_deleted,
                                  'new': add_new})

                    s = json.dumps(d, ensure_ascii=False, indent=2)

                    thread = threading.Thread(target=self.run_after, args=(s, after), daemon=True)
                    thread.start()
                    self.threads.append(thread)

            logger.info(f'Done checking {playlist_id}')

        after = self.config.get('after_all')
        if after:
            # TODO do when it when you need it
            pass

        if self.threads:
            timeout = 900/len(self.threads)
            for thread in self.threads:
                thread.join(timeout=timeout)

            if list(filter(lambda t: t.is_alive(), self.threads)):
                logger.warning('Threads open even after 15min. Force closing')
                exit()

