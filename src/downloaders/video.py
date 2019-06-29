import logging
import os
import time
from random import uniform

import youtube_dl

logger = logging.getLogger('debug')


SLEEP = [3, 6]

BASE_OPTS = {
    'format': 'bestvideo+bestaudio',
    'merge_output_format': 'mp4',
    'outtmpl': '%(format_id)s-%(id)s.%(ext)s',
    'restrictfilenames': True,
    'noplaylist': True,
    'nocheckcertificate': True,
    'ignoreerrors': False,
    'logtostderr': False,
    'noprogress': True,
    'quiet': True,
    'no_warnings': True,
    'default_search': 'auto',
    'nooverwrites': True,
    'prefer_ffmpeg': True,
    'logger': logger,
    'no_color': True,
}


def download_video(video, row, opts):
    """

    Args:
        video (src.video.BaseVideo): Video object of the row
        row: Database row with the columns download_filename, site
        opts (dict): format options
    Returns:

    """

    path = os.path.join('data', 'videos', str(row['site']))
    os.makedirs(path, exist_ok=True)

    file = row['download_filename']

    if row['downloaded_format'] == opts['format'] and os.path.exists(file):
        return

    opts['outtmpl'] = os.path.join(path, BASE_OPTS['outtmpl'])

    try:
        with youtube_dl.YoutubeDL({**BASE_OPTS, **opts}) as ytdl:
            res = ytdl.extract_info(video.link)
            new_file = ytdl.prepare_filename(res)

    except:
        logger.exception('Failed to dl vid')
        return

    time.sleep(uniform(*SLEEP))

    return new_file
