import io
import logging
import os
import re
import subprocess
import time
from dataclasses import dataclass
from random import uniform

import yt_dlp
from yt_dlp.utils import replace_extension, Popen, PostProcessingError

from src.config import MinMax
from src.config import get_yt_dlp_options
from src.db import models

logger = logging.getLogger('debug')
override_opts = get_yt_dlp_options()

SLEEP = MinMax(min=3, max=6)


@dataclass
class DownloadInfo:
    filename: str
    downloaded_format: str
    success: bool
    thumbnail_path: str = None
    info_path: str = None
    subtitle_paths: list[str] = None
    blocked: bool = False

    @classmethod
    def failed(cls, blocked=False):
        return cls('', '', False, blocked=blocked)


class Srv3SubtitlesConvertorAss(yt_dlp.FFmpegSubtitlesConvertorPP):
    def __init__(self, downloader=None, keep_originals=True, converter_path=None):
        super().__init__(downloader=downloader, format='ass')
        self.keep_originals = keep_originals
        self.converter_path = converter_path
        self._ext = 'srv3'

    def run(self, info):
        if not self.converter_path:
            files, info = super().run(info)
            if self.keep_originals:
                return [], info
            return files, info

        subs = info.get('requested_subtitles')
        new_ext = self.format
        if subs is None:
            self.to_screen('There aren\'t any subtitles to convert')
            return [], info

        self.to_screen('Converting subtitles using YTSubConverter')
        sub_filenames = []
        converted = 0
        subs_count = len(subs.keys())

        for lang, sub in subs.items():
            if not os.path.exists(sub.get('filepath', '')):
                self.report_warning(f'Skipping embedding {lang} subtitle because the file is missing')
                continue
            ext = sub['ext']
            if ext == new_ext:
                self.to_screen('Subtitle file for %s is already in the requested format' % new_ext)
                continue

            # This postprocessor only supports one kind of subtitle
            elif ext != self._ext:
                continue

            old_file = sub['filepath']

            if not self.keep_originals:
                sub_filenames.append(old_file)

            new_file = replace_extension(old_file, new_ext)

            cmd = [self.converter_path, old_file, new_file, '--visual']
            self.write_debug('YTSubConverter command line: %s' % yt_dlp.utils.shell_quote(cmd))
            p = Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                      stdin=subprocess.PIPE)
            stdout, stderr = p.communicate_or_kill()
            if p.returncode not in (0,):
                stderr = stderr.decode('utf-8', 'replace').strip()
                self.write_debug(stderr)
                raise PostProcessingError(stderr.split('\n')[-1])

            converted += 1

            with io.open(new_file, 'rt', encoding='utf-8') as f:
                subs[lang] = {
                    'ext': new_ext,
                    'data': f.read(),
                    'filepath': new_file,
                }

            info['__files_to_move'][new_file] = replace_extension(
                info['__files_to_move'][sub['filepath']], new_ext)

        if converted != subs_count:
            files, info = super().run(info)
            if not self.keep_originals:
                sub_filenames.extend(files)

        return sub_filenames, info


class SaveFilenamesPP(yt_dlp.postprocessor.PostProcessor):
    """Saves filenames (thumbnail and subtitles) to a DownloadInfo object before they are removed from the info dict."""
    def __init__(self, download_info: DownloadInfo, downloader=None):
        super().__init__(downloader)
        self.download_info = download_info

    @staticmethod
    def get_thumbnail_path(info):
        if not info.get('thumbnails'):
            return None

        idx = next((
            -i for i, t in enumerate(info['thumbnails'][::-1], 1) if t.get('filepath')
        ), None)

        if idx is None:
            return None

        thumbnail_filename = info['thumbnails'][idx]['filepath']
        if not os.path.exists(yt_dlp.utils.encodeFilename(thumbnail_filename)):
            return None

        return thumbnail_filename

    @staticmethod
    def get_filepaths(data: list[dict]) -> list[str]:
        return [t['filepath'] for t in data if 'filepath' in t]

    def get_subtitle_paths(self, info):
        filepaths = set()

        if subtitles := info.get('subtitles'):
            for subs in subtitles.values():
                filepaths.update(self.get_filepaths(subs))

        if requested_subs := info.get('requested_subtitles'):
            filepaths.update(
                self.get_filepaths(requested_subs.values())
            )

        if not filepaths:
            return None

        return list(filepaths)

    def run(self, info):
        self.download_info.thumbnail_path = self.get_thumbnail_path(info)
        self.download_info.subtitle_paths = self.get_subtitle_paths(info)
        return [], info


BASE_OPTS = {
    # Max title length 200 bytes
    'outtmpl': '%(title).200B [%(id)s].%(ext)s',
    'format': 'bv*+ba/b',
    'writeinfojson': True,
    'writesubtitles': True,
    'subtitlesformat': 'ass/srv3/ttml/best',
    'writethumbnail': True,
    'subtitleslangs': ['all'],
    'postprocessors': [
        {
            'key': 'EmbedThumbnail',
            # already_have_thumbnail = True prevents the file from being deleted after embedding
            'already_have_thumbnail': True
        },
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
        },
        {
            'key': 'MetadataParser',
            # Remove automatic captions from info json
            'actions': [yt_dlp.MetadataFromFieldPP.to_action(':(?P<automatic_captions>)')],
            'when': 'pre_process'
        },
    ],
    'merge_output_format': 'mp4',
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
    'fragment_retries': 10,
    'continuedl': False,
    'retries': 10,
    **override_opts
}


def download_video(video, row: models.Video, opts, sleep: MinMax = SLEEP) -> DownloadInfo:
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
        opts['overwrites'] = True

    # Override default format
    if row.download_format:
        opts['format'] = row.download_format

    logger.info(f'Downloading {video.video_id}')
    # The default template should not cause any filename collision problems between sites
    # as the chance of the title and id being the same on multiple sites is low (unless the video is exactly the same).
    outtmpl = BASE_OPTS.get('outtmpl', yt_dlp.utils.DEFAULT_OUTTMPL['default'])
    opts['outtmpl'] = os.path.join(path, outtmpl)

    try:
        with yt_dlp.YoutubeDL({**BASE_OPTS, **opts}) as ytdl:
            dl_info = DownloadInfo.failed()
            ytdl.add_post_processor(Srv3SubtitlesConvertorAss(converter_path=os.getenv('YT_SUBS_CONVERTER', None)), when='before_dl')
            ytdl.add_post_processor(SaveFilenamesPP(dl_info), when='after_move')

            info = ytdl.sanitize_info(ytdl.extract_info(video.link))
            new_file = ytdl.prepare_filename(info)
            downloaded_format = info.get('format', opts.get('format', BASE_OPTS.get('format', 'default')))

            dl_info.filename = new_file
            dl_info.success = True
            dl_info.downloaded_format = downloaded_format
            dl_info.info_path = ytdl.prepare_filename(info, 'infojson')
    except yt_dlp.DownloadError as e:
        time.sleep(uniform(sleep.min, sleep.max))

        blocked = re.search(r'blocked in your|copyright grounds|video unavailable', e.msg, re.I) is not None
        if blocked:
            logger.warning(f'Video was blocked  in your country. {e.msg}')
        else:
            logger.exception('Failed to dl vid')

        return DownloadInfo.failed(blocked=blocked)
    except:
        logger.exception('Failed to dl vid')
        time.sleep(uniform(sleep.min, sleep.max))
        return DownloadInfo.failed()

    time.sleep(uniform(sleep.min, sleep.max))

    return dl_info
