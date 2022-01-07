from abc import ABC, abstractmethod
from typing import Optional

from src.api import YTApi, Part
from src.channel import YTChannel
from src.video import YTVideo


class BasePlaylist(ABC):
    def __init__(self, db, **options):
        self._db = db

    @property
    @abstractmethod
    def url_format(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def channel_url_format(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def playlist_url_format(self):
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

    @abstractmethod
    def get_channels(self, channel_ids):
        raise NotImplementedError


class YTPlaylist(BasePlaylist):
    def __init__(self, db, api: YTApi, playlist_id: str, **options):
        super().__init__(db, **options)
        self.playlist_id = playlist_id
        self._api = api

    @property
    def api(self):
        return self._api

    @property
    def url_format(self):
        return 'https://www.youtube.com/watch?v=%s'

    @property
    def channel_url_format(self):
        return 'https://www.youtube.com/channel/%s'

    @property
    def playlist_url_format(self):
        return 'https://www.youtube.com/playlist?list=%s'

    @staticmethod
    def vids2set(videos) -> set[YTVideo]:
        video_set = {YTVideo(video['id'], **video) for video in videos}
        return video_set

    @staticmethod
    def playlistvids2set(videos):
        video_set = {YTVideo(video['contentDetails']['videoId'], **video) for video in videos}
        return video_set

    def get_playlist_info(self):
        return self.api.playlist_info(self.playlist_id, Part.Snippet)

    def get_videos(self, already_checked: dict) -> Optional[tuple[set[YTVideo], set[YTVideo], set[YTVideo]]]:
        """
        Finds new unchecked, deleted and already checked videos in the playlist.
        Checks deleted videos by getting all items in the playlist first (this will return all video ids even of the deleted videos)
        and then getting the video details for all videos.
        Deleted videos are the videos that are not included in the latter query.
        """
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

    def get_deleted(self, new: set[YTVideo], old: set[YTVideo], checked_vids: set[YTVideo]) -> set[YTVideo]:
        deleted = old - new - checked_vids
        for vid in deleted:
            vid.data = {'snippet': {}}

        return deleted

    def get_channels(self, channel_ids) -> Optional[list[YTChannel]]:
        js = self.api.channel_info(list(channel_ids), Part.Snippet)
        if not js:
            return

        items = []
        for item in js['items']:
            channel_id = item['id']
            items.append(YTChannel(channel_id, item))

        return items
