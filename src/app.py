from src.enums import Sites
import pymysql.cursors
from src.api import YTApi
from src.playlist import YTPlaylist
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
        sql = 'INSERT INTO `videos` (`video_id`, `title`, `description`, `published_at`, `site`) VALUES ' \
              f'(%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE title=IF(`title`!=VALUES(`title`), VALUES(`title`), `title`), ' \
              'description=IF(`description`!=VALUES(`description`), VALUES(`description`), `description`), deleted=FALSE'

        values = ((vid.video_id, vid.title, vid.description, self.datetime2sql(vid.published_at), site)
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

    def run_after(self, data, cmds):
        for after in cmds:
            p = subprocess.Popen(shlex.split(after), stdin=subprocess.PIPE)
            p.stdin.write(data.encode('utf-8'))
            p.communicate()

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

            if site == Sites.Youtube:
                playlist_checker = YTPlaylist(self.db, self.yt_api, playlist_id)
                if not playlist_data:
                    logger.info('New playlist getting playlist info')
                    info = playlist_checker.get_playlist_info()
                    if not info:
                        continue

                    playlist_data['id'] = self.add_playlist(playlist_id, info['snippet']['title'], site)

                # Get videos
                old = self.get_playlist_video_ids(playlist_data['id'])
                items, deleted, already_checked = playlist_checker.get_videos(self.already_checked[site])

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

                # After processing of data by external scripts
                after = playlist.get('after', [])
                after.extend(self.config.get('after', []))  # Default after command

                if after:
                    old = [d['video_id'] for d in old]
                    new = items - {k for k, v in self.all_vids[site].items() if
                                   v in old}
                    d = {'deleted': [{'id': vid.video_id} for vid in deleted],
                         'new': [{'id': vid.video_id} for vid in new],
                         'url_format': playlist_checker.url_format}
                    s = json.dumps(d, ensure_ascii=False, indent=2)

                    thread = threading.Thread(target=self.run_after, args=(s, after), daemon=True)
                    thread.start()
                    self.threads.append(thread)

            logger.info(f'Done checking {playlist_id}')

        after = self.config.get('after')
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

