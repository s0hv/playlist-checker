import logging
import os
import time
from random import uniform
from typing import Optional

import yt_dlp

from src.config import MinMax
from src.config.yt_dlp import get_yt_dlp_options

logger = logging.getLogger('debug')
override_opts = get_yt_dlp_options()

SLEEP = MinMax(min=3, max=6)

BASE_OPTS = {
    'format': 'bv*+ba/b',
    'writeinfojson': True,
    'postprocessors': [
        {
            'key': 'MetadataParser',
            # Remove automatic_captions from info json
            'actions': [yt_dlp.MetadataFromFieldPP.to_action(':(?P<automatic_captions>)')],
            # Run this immediately after extraction is complete
            'when': 'pre_process'
        },
        {
            # Embed metadata in video using ffmpeg.
            # ℹ️ See yt_dlp.postprocessor.FFmpegMetadataPP for the arguments it accepts
            'key': 'FFmpegMetadata',
            'add_chapters': True,
            'add_metadata': True,
            'add_infojson': True
        },
        {
            'key': 'FFmpegEmbedSubtitle',
            # already_have_subtitle = True prevents the file from being deleted after embedding
            'already_have_subtitle': True
        }
    ],
    'merge_output_format': 'mkv',
    'restrictfilenames': True,
    'noplaylist': True,
    'nocheckcertificate': True,
    'ignoreerrors': False,
    'logtostderr': False,
    'noprogress': True,
    'quiet': True,
    'no_warnings': True,
    'nooverwrites': True,
    'logger': logger,
    'no_color': True,
    **override_opts
}


def download_video(video, row, opts, sleep: MinMax = SLEEP) -> Optional[str]:
    """

    Args:
        video (src.video.BaseVideo): Video object of the row
        row: Database row with the columns download_filename, site
        opts (dict): format options
        sleep: How long to sleep after download
    Returns:

    """
    path = os.path.join('data', 'videos', str(row['site']))
    os.makedirs(path, exist_ok=True)

    file = row['download_filename']

    if row['downloaded_format'] == opts['format'] and os.path.exists(file):
        return

    logger.info(f'Downloading {video.video_id}')
    outtmpl = BASE_OPTS.get('outtmpl', yt_dlp.utils.DEFAULT_OUTTMPL['default'])
    opts['outtmpl'] = os.path.join(path, outtmpl)

    try:
        with yt_dlp.YoutubeDL({**BASE_OPTS, **opts}) as ytdl:
            res = ytdl.sanitize_info(ytdl.extract_info(video.link))
            new_file = ytdl.prepare_filename(res)

    except:
        logger.exception('Failed to dl vid')
        return

    time.sleep(uniform(sleep.min, sleep.max))

    return new_file
