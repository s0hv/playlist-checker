from abc import ABC, abstractmethod


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
    def tags(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def published_at(self):
        raise NotImplementedError

    def __eq__(self, other):
        return self.video_id == other.video_id

    def __hash__(self):
        return hash(self.video_id)


class YTVideo(BaseVideo):
    def __init__(self, video_id, **data):
        super().__init__(video_id)
        self.data = data

    def title(self):
        return self.data['snippet']['title']

    def link(self):
        return 'https://www.youtube.com/watch?v=%s' % self.video_id

    @property
    def description(self):
        return self.data['snippet'].get('description', '')

    def tags(self):
        return self.data['snippet'].get('tags', [])

    def published_at(self):
        return self.data['snippet'].get('publishedAt')
