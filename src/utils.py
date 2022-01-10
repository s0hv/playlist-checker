import os
from typing import Optional


def get_yt_thumb(thumbnails):
    for quality in ['maxres', 'standard', 'high', 'medium', 'default']:
        thumb = thumbnails.get(quality)
        if thumb:
            return thumb['url']


def generate_extra_files(subtitles: list[str] = None) -> Optional[dict[str, str | list[str]]]:
    d = {}
    if subtitles:
        d['subtitles'] = subtitles

    return d or None


def get_filename(path: str) -> str:
    _, filename = os.path.split(path)
    return filename
