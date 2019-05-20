import re
from abc import ABC, abstractmethod
from datetime import datetime

from src.enums import Sites
from src.utils import get_yt_thumb

hashtag_regex = re.compile('#\w+')


class BaseVideo(ABC):
    SITE = None

    def __init__(self, video_id, data):
        self.video_id = video_id
        self.data = data

    @property
    @abstractmethod
    def title(self):
        raise NotImplementedError

    @title.setter
    def title(self, value):
        raise NotImplementedError

    @property
    @abstractmethod
    def link(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def description(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def channel_name(self):
        raise NotImplementedError

    @channel_name.setter
    def channel_name(self, value):
        raise NotImplementedError

    @property
    @abstractmethod
    def channel_id(self):
        raise NotImplementedError

    @channel_id.setter
    def channel_id(self, value):
        raise NotImplementedError

    @property
    @abstractmethod
    def channel_url(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def tags(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def thumbnail(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def published_at(self):
        raise NotImplementedError

    def to_dict(self):
        raise NotImplementedError

    def __eq__(self, other):
        if isinstance(other, BaseVideo):
            return other.SITE == self.SITE and self.video_id == other.video_id
        else:
            return self.video_id == other

    def __hash__(self):
        return hash(self.video_id)

    def __repr__(self):
        return str(self.video_id)


class YTVideo(BaseVideo):
    SITE = Sites.Youtube

    def __init__(self, video_id, **data):
        super().__init__(video_id, data)
        if 'snippet' not in data:
            data['snippet'] = {}

        self._hashtags = hashtag_regex.findall(self.description)[:10]

    @property
    def title(self):
        return self.data['snippet'].get('title')

    @title.setter
    def title(self, title):
        self.data['snippet']['title'] = title

    @property
    def link(self):
        return 'https://www.youtube.com/watch?v=%s' % self.video_id

    @property
    def channel_name(self):
        return self.data['snippet'].get('channelTitle')

    @channel_name.setter
    def channel_name(self, name):
        self.data['snippet']['channelTitle'] = name

    @property
    def channel_id(self):
        return self.data['snippet'].get('channelId')

    @property
    def thumbnail(self):
        thumbs = self.data['snippet'].get('thumbnails')
        if not thumbs:
            return

        return get_yt_thumb(thumbs)

    @channel_id.setter
    def channel_id(self, chnl_id):
        self.data['snippet']['channelId'] = chnl_id

    @property
    def channel_url(self):
        if self.channel_id:
            return 'https://www.youtube.com/channel/%s' % self.channel_id

    @property
    def description(self):
        return self.data.get('snippet', {}).get('description', '')

    @property
    def tags(self):
        tags = self.data['snippet'].get('tags', [])
        tags.extend(self._hashtags)
        return list(filter(lambda t: len(t) < 191, tags))

    @property
    def published_at(self):
        t = self.data['snippet'].get('publishedAt')
        if t:
            t = datetime.strptime(t, '%Y-%m-%dT%H:%M:%S.%fZ')

        return t

    def to_dict(self):
        return {'id': self.video_id,
                'title': self.title or 'Deleted video',
                'channel_name': self.channel_name,
                'channel_id': self.channel_id}
