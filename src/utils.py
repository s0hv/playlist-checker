def get_yt_thumb(thumbnails):
    for quality in ['maxres', 'standard', 'high', 'medium', 'default']:
        thumb = thumbnails.get(quality)
        if thumb:
            return thumb['url']
