import os
import time

import requests

from src.enum import Site

SITE_OPTIONS = {
    0: {'CHUNK_SIZE': 5, 'SLEEP': 2}
}


def download_thumbnail(video, site: Site):
    """
    Downloads a thumbnail of the vid if it doesn't exist
    Args:
        video (src.video.BaseVideo): The video object
        site: Which site this video belongs to

    Returns:
        bool or None: Triple state bool denoting if downloading was a success (True),
            failure (False) or file exists already (None)
    """
    url = video.thumbnail
    if not url:
        return False

    # We hope all images are jpg
    path = os.path.join('data', str(site.value), video.video_id + '.jpg')

    if os.path.exists(path):
        return None

    with open(path, 'wb') as f:
        resp = requests.get(url, stream=True)

        if not resp.ok:
            return False

        for chunk in resp.iter_content(4096):
            if not chunk:
                break

            f.write(chunk)


def bulk_download_thumbnails(videos, site: Site):
    CHUNK_SIZE = SITE_OPTIONS[site]['CHUNK_SIZE']
    SLEEP = SITE_OPTIONS[site]['SLEEP']

    path = os.path.join('data', str(site))
    os.makedirs(path, exist_ok=True)

    i = 0
    for vid in videos:
        res = download_thumbnail(vid, site)
        if res is None:
            continue

        i += 1

        if i % CHUNK_SIZE == 0:
            time.sleep(SLEEP)
