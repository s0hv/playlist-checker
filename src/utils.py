import os


def get_yt_thumb(thumbnails):
    for quality in ['maxres', 'standard', 'high', 'medium', 'default']:
        thumb = thumbnails.get(quality)
        if thumb:
            return thumb['url']


def get_filename(path: str) -> str:
    _, filename = os.path.split(path)
    return filename
