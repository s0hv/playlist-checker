import logging
import mimetypes
import os
import re
import sys
from argparse import ArgumentParser
from collections import Counter
from mimetypes import guess_type

import dotenv
import psycopg

from src.db import models
from src.db.dbutils import DbUtils

dir_path = os.path.dirname(os.path.realpath(__file__))
os.chdir(dir_path)

dotenv.load_dotenv()

from src.enum import S3ObjectType
from src.config import Config

subtitle_formats = {'.ass', '.srv1', '.srv2', '.srv3', '.srt', '.ttml', '.vtt', '.stl', '.dfxp', '.sami', '.ssa'}


image_formats = ['webp']
for im_f in image_formats:
    mimetypes.add_type(f'image/{im_f}', f'.{im_f}', strict=False)


def get_ext(path: str) -> [str, str]:
    filepath, ext = os.path.splitext(path)
    return filepath, ext


def get_s3_type(path: str) -> S3ObjectType:
    filepath, ext = get_ext(path)
    if ext in subtitle_formats:
        return S3ObjectType.subtitle

    mimetype, _ = guess_type(path, strict=False)

    if 'image/' in mimetype:
        return S3ObjectType.thumbnail

    if 'video/' in mimetype:
        return S3ObjectType.video

    if '/json' in mimetype:
        if filepath.endswith('info'):
            return S3ObjectType.metadata
        if filepath.endswith('live_chat'):
            return S3ObjectType.subtitle

    if 'audio/' in mimetype:
        return S3ObjectType.audio

    return S3ObjectType.other


if __name__ == '__main__':
    logger = logging.getLogger('debug')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:[%(module)s] %(message)s'))
    logger.addHandler(handler)

    logs_dir = os.getenv('LOGS_DIR', dir_path)
    handler = logging.FileHandler(filename=os.path.join(logs_dir, 'debug.log'), encoding='utf-8', mode='a')
    handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:[%(module)s] %(message)s'))
    logger.addHandler(handler)

    parser = ArgumentParser()
    parser.add_argument('-d', '--dry-run', action='store_true', default=False)
    parser.add_argument('-a', '--upload-all', action='store_true', default=False,
                        help='''If this parameter is given uploads all files,
                        even if those files could not be linked to a video id.
                        However those uploads cannot be further tracked back to the database.''')

    args = parser.parse_args(sys.argv[1:])

    config = Config.load()
    data_dir = os.path.join(dir_path, 'data')
    video_dir = os.path.join(data_dir, 'videos')
    counter = Counter()

    video_files = {}

    for site in os.listdir(video_dir):
        site_path = os.path.join(video_dir, site)
        for file in os.listdir(site_path):
            s3_type = get_s3_type(file)
            counter.update([s3_type])

            filename, ext = os.path.splitext(file)
            filename = re.sub(r'(\.info|\.live_chat|\.\w{2})$', '', filename)
            if filename not in video_files:
                video_files[filename] = {}

            data = video_files[filename]

            strict = False
            key = s3_type
            match s3_type:
                case S3ObjectType.video | S3ObjectType.thumbnail | S3ObjectType.metadata | S3ObjectType.audio:
                    strict = True

            if s3_type == S3ObjectType.other:
                print(f'File {file} classified as S3ObjectType.other')

            if strict and key in data:
                raise ValueError(f'Multiple files of same type for file {file}.\n{data}')

            file_path = os.path.join(site_path, file)
            if strict:
                data[key] = file_path
            else:
                if key in data:
                    data[key].append(file_path)
                else:
                    data[key] = [file_path]

    print('')
    print('File type statistics')
    for name, count in counter.most_common():
        print(f'{name}: {count}')

    sql = '''
    SELECT id, downloaded_filename, site, video_id FROM videos WHERE downloaded_filename IS NOT NULL
    '''
    with psycopg.connect(config.db_conn_string) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            filename2id = {os.path.split(f)[1]: (v_id, site, video_id) for v_id, f, site, video_id in cur}

    not_found = {}

    for k, v in list(video_files.items()):
        # If only audio file found use that as the video as well
        if S3ObjectType.video not in v and S3ObjectType.audio in v:
            v[S3ObjectType.video] = v[S3ObjectType.audio]

        if S3ObjectType.video not in v:
            not_found[k] = v
            if not args.upload_all:
                video_files.pop(k)
            continue

        video_file = os.path.split(v[S3ObjectType.video])[1]
        if not (ids := filename2id.get(video_file, None)):
            not_found[k] = v
            if not args.upload_all:
                video_files.pop(k)
            continue

        v['id'] = ids[0]
        v['site'] = ids[1]
        v['video_id'] = ids[2]

    print(f'{len(video_files)} matches found from database. {len(not_found)} videos could not be linked to a video id.')

    if args.dry_run:
        exit()

    from src.app import PlaylistChecker

    with psycopg.connect(config.db_conn_string) as conn:
        db = DbUtils(conn)
        checker = PlaylistChecker(config=config)

        for d in video_files.values():
            base_tags = {}
            site = d.get('site')
            video_db_id = d.get('id')

            if video_file := d.get(S3ObjectType.video):
                s3_file = checker.upload_and_delete_file(video_file, base_tags, S3ObjectType.video)

                if s3_file and video_db_id:
                    db.update_filename(s3_file, video_db_id)

            info_file = checker.upload_and_delete_file(d.get(S3ObjectType.metadata), base_tags, S3ObjectType.metadata)
            thumbnail_file = checker.upload_and_delete_file(d.get(S3ObjectType.thumbnail), base_tags, S3ObjectType.thumbnail)

            # If video and audio are the same file the file has already been deleted.
            # just use the normal uploaded filename
            if video_file and video_file == d.get(S3ObjectType.audio):
                audio_file = video_file
            else:
                audio_file = checker.upload_and_delete_file(d.get(S3ObjectType.audio), base_tags, S3ObjectType.audio)

            subs = []
            if subtitle_paths := d.get(S3ObjectType.subtitle):
                for sub in subtitle_paths:
                    sub_path = checker.upload_and_delete_file(sub, base_tags, S3ObjectType.subtitle)
                    if sub_path is not None:
                        subs.append(sub_path)

            if video_db_id:
                extra_files = models.VideoExtraFiles(
                    video_id=video_db_id,
                    thumbnail=thumbnail_file,
                    info_json=info_file,
                    audio_file=audio_file,
                    subtitles=subs
                )
                logger.info(f'Updating extra files with object {extra_files}')
                db.update_extra_files(extra_files)

        sql = '''
        SELECT v.site, v.video_id, v.id FROM videos v
        LEFT JOIN extra_video_files evf ON v.id = evf.video_id
        WHERE evf.thumbnail IS NULL
        '''

        with checker.conn.cursor() as cur:
            cur.execute(sql)
            missing_thumbs = cur.fetchall()

        for site, video_id, id_ in missing_thumbs:
            thumbs_path = os.path.join(data_dir, str(site))
            thumbnail_file = os.path.join(thumbs_path, video_id + '.jpg')  # It's all jpeg
            if os.path.exists(thumbnail_file):
                base_tags = {}
                thumbnail = checker.upload_and_delete_file(thumbnail_file, base_tags, S3ObjectType.thumbnail)

                if thumbnail:
                    db.update_extra_files(models.VideoExtraFiles(
                        video_id=id_,
                        thumbnail=thumbnail
                    ))
