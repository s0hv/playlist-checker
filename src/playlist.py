from abc import ABC, abstractmethod
from src.api import YTApi, Part
from src.video import YTVideo


class BasePlaylist(ABC):
    def __init__(self, db, **options):
        self._db = db

    @property
    def db(self):
        return self._db

    @abstractmethod
    def get_videos(self):
        raise NotImplementedError

    @abstractmethod
    def get_deleted(self, new, old):
        raise NotImplementedError


class YTPlaylist(BasePlaylist):
    def __init__(self, db, api: YTApi, playlist_id, **options):
        super().__init__(db, **options)
        self.playlist_id = playlist_id
        self._api = api

    @property
    def api(self):
        return self._api

    @staticmethod
    def vids2set(videos):
        video_set = {YTVideo(video['id'], **video) for video in videos}
        return video_set

    @staticmethod
    def playlistvids2set(videos):
        video_set = {YTVideo(video['snippet']['resourceId']['videoId'], **video) for video in videos}
        return video_set

    def get_videos(self):
        js = self.api.playlist_items(self.playlist_id, Part.combine(Part.ID, Part.ContentDetails))
        if js is None:
            return

        items = [v['snippet']['resourceId']['videoId'] for v in js['items']]
        js = self.api.video_info(items, Part.Snippet)
        if not js:
            return

        items = js['items']

        return self.get_deleted(self.vids2set(items), self.playlistvids2set(js['items']))

    def get_deleted(self, new: set, old: set):
        deleted = old - new
        return deleted
