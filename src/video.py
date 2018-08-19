from abc import ABC, abstractmethod
import re
from datetime import datetime

hashtag_regex = re.compile('#\w+')


class BaseVideo(ABC):
    def __init__(self, video_id):
        self.video_id = video_id

    @property
    @abstractmethod
    def title(self):
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

    @property
    @abstractmethod
    def channel_id(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def tags(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def published_at(self):
        raise NotImplementedError

    def __eq__(self, other):
        if isinstance(other, BaseVideo):
            return self.video_id == other.video_id
        else:
            return self.video_id == other

    def __hash__(self):
        return hash(self.video_id)


class YTVideo(BaseVideo):
    def __init__(self, video_id, **data):
        super().__init__(video_id)
        self.data = data
        self._hashtags = hashtag_regex.findall(self.description)[:10]

    @property
    def title(self):
        return self.data['snippet'].get('title')

    @property
    def link(self):
        return 'https://www.youtube.com/watch?v=%s' % self.video_id

    @property
    def channel_name(self):
        return self.data['snippet'].get('channelTitle')

    @property
    def channel_id(self):
        return self.data['snippet'].get('channelId')

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
