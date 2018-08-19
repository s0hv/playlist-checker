from abc import ABC, abstractmethod
from src.api import YTApi, Part
from src.video import YTVideo


class BasePlaylist(ABC):
    def __init__(self, db, **options):
        self._db = db

    @property
    @abstractmethod
    def url_format(self):
        raise NotImplementedError

    @property
    def db(self):
        return self._db

    @abstractmethod
    def get_videos(self, already_checked: set):
        raise NotImplementedError

    @abstractmethod
    def get_deleted(self, new, old, checked_vids):
        raise NotImplementedError


class YTPlaylist(BasePlaylist):
    def __init__(self, db, api: YTApi, playlist_id, **options):
        super().__init__(db, **options)
        self.playlist_id = playlist_id
        self._api = api

    @property
    def api(self):
        return self._api

    @property
    def url_format(self):
        return 'https://www.youtube.com/watch?v=%s'

    @staticmethod
    def vids2set(videos):
        video_set = {YTVideo(video['id'], **video) for video in videos}
        return video_set

    @staticmethod
    def playlistvids2set(videos):
        video_set = {YTVideo(video['contentDetails']['videoId'], **video) for video in videos}
        return video_set

    def get_playlist_info(self):
        return self.api.playlist_info(self.playlist_id, Part.Snippet)

    def get_videos(self, already_checked: dict):
        js = self.api.playlist_items(self.playlist_id, Part.combine(Part.ID, Part.ContentDetails))
        if js is None:
            return

        checked_items = set()
        items = []
        all_items = js['items']
        for vid in all_items:
            vid_id = vid['contentDetails']['videoId']
            if vid_id in already_checked:
                checked_items.add(YTVideo(vid_id))
            else:
                items.append(vid_id)

        js = self.api.video_info(items, Part.Snippet)
        if not js:
            return

        items = self.vids2set(js['items'])

        deleted = self.get_deleted(items, self.playlistvids2set(all_items), checked_items)
        items = items - deleted

        return items, deleted, checked_items

    def get_deleted(self, new: set, old: set, checked_vids: set):
        deleted = old - new - checked_vids
        for vid in deleted:
            vid.data = None

        return deleted
