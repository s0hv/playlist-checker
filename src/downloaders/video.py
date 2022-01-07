import logging
import os
import time
from dataclasses import dataclass
from random import uniform
from typing import Optional

import yt_dlp

from src.config import MinMax
from src.config import get_yt_dlp_options
from src.db import models

logger = logging.getLogger('debug')
override_opts = get_yt_dlp_options()

SLEEP = MinMax(min=3, max=6)


class FFmpegSubtitlesConvertorKeepOriginalsPP(yt_dlp.FFmpegSubtitlesConvertorPP):
    def run(self, info):
        # Ignore the generated subtitle files while processing.
        # We want to keep those as it is a lossy conversion.
        # The conversion must be done because ttml files don't really work in non web apps.
        # Some information such as positions will be lost during this conversion.
        _, info = super().run(info)
        return [], info


BASE_OPTS = {
    'format': 'bv*+ba/b',
    'writeinfojson': True,
    'writesubtitles': True,
    'subtitlesformat': 'ass/ttml/best',
    'postprocessors': [
        {
            # Embed metadata in video using ffmpeg.
            # ℹ️ See yt_dlp.postprocessor.FFmpegMetadataPP for the arguments it accepts
            'key': 'FFmpegMetadata',
            'add_chapters': True,
            'add_metadata': True,
            'add_infojson': False
        },
        {
            'key': 'FFmpegEmbedSubtitle',
            # already_have_subtitle = True prevents the file from being deleted after embedding
            'already_have_subtitle': True
        }
    ],
    'merge_output_format': 'mp4',
    'restrictfilenames': True,
    'noplaylist': True,
    'nocheckcertificate': True,
    'ignoreerrors': False,
    'logtostderr': False,
    'noprogress': True,
    'quiet': True,
    'no_warnings': True,
    'overwrites': False,
    'logger': logger,
    'no_color': True,
    **override_opts
}


@dataclass
class DownloadInfo:
    filename: str
    downloaded_format: str
    success: bool

    @classmethod
    def failed(cls):
        return cls('', '', False)


def download_video(video, row: models.Video, opts, sleep: MinMax = SLEEP) -> Optional[DownloadInfo]:
    """

    Args:
        video (src.video.BaseVideo): Video object of the row.
        row: Database row with the columns download_filename, site
        opts (dict): format options
        sleep: How long to sleep after download
    Returns:

    """
    path = os.path.join('data', 'videos', str(row.site))
    os.makedirs(path, exist_ok=True)

    if row.force_redownload:
        opts['overwrite'] = True
    elif row.downloaded_format:
        return

    # Override default format
    if row.download_format:
        opts['format'] = row.download_format

    logger.info(f'Downloading {video.video_id}')
    outtmpl = BASE_OPTS.get('outtmpl', yt_dlp.utils.DEFAULT_OUTTMPL['default'])
    opts['outtmpl'] = os.path.join(path, outtmpl)

    try:
        with yt_dlp.YoutubeDL({**BASE_OPTS, **opts}) as ytdl:
            ytdl.add_post_processor(FFmpegSubtitlesConvertorKeepOriginalsPP(format='ass'), when='before_dl')
            res = ytdl.sanitize_info(ytdl.extract_info(video.link))
            new_file = ytdl.prepare_filename(res)

    except:
        logger.exception('Failed to dl vid')
        return DownloadInfo.failed()

    time.sleep(uniform(sleep.min, sleep.max))

    downloaded_format = opts.get('format', BASE_OPTS.get('format', 'default'))

    return DownloadInfo(
        filename=new_file,
        success=True,
        downloaded_format=downloaded_format
    )
