from abc import ABC, abstractmethod
from datetime import datetime

from src.utils import get_yt_thumb


class BaseChannel(ABC):
    def __init__(self, channel_id):
        self.channel_id = channel_id

    @property
    @abstractmethod
    def name(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def link(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def thumbnail(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def created_at(self):
        raise NotImplementedError

    def __eq__(self, other):
        if isinstance(other, BaseChannel):
            return self.channel_id == other.channel_id
        else:
            return self.channel_id == other

    def __hash__(self):
        return hash(self.channel_id)


class YTChannel(BaseChannel):
    def __init__(self, channel_id, data):
        super().__init__(channel_id)
        self._data = data

    @property
    def data(self):
        return self._data

    @property
    def name(self):
        return self.data['snippet'].get('title')

    @property
    def link(self):
        return 'https://www.youtube.com/channel/%s' % self.channel_id

    @property
    def thumbnail(self):
        thumbs = self.data['snippet'].get('thumbnails')
        if not thumbs:
            return

        return get_yt_thumb(thumbs)

    @property
    def created_at(self):
        t = self.data['snippet'].get('publishedAt')
        if t:
            t = datetime.strptime(t, '%Y-%m-%dT%H:%M:%S.%fZ')

        return t
