from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from .DbBase import DbBase


@dataclass(eq=False)
class PartialVideo(DbBase):
    video_id: str
    site: Optional[int] = None

    def __eq__(self, other):
        if isinstance(other, Video):
            return self.id == other.id

        return False


@dataclass(eq=False)
class Video(PartialVideo):
    title: Optional[str] = None
    description: Optional[str] = None
    published_at: Optional[datetime] = None
    deleted: Optional[bool] = None
    deleted_at: Optional[datetime] = None
    alternative: Optional[str] = None
    thumbnail: Optional[str] = None
    download: Optional[bool] = None
    download_format: Optional[str] = None
    downloaded_filename: Optional[str] = None
    downloaded_format: Optional[str] = None
    force_redownload: Optional[bool] = None

    @property
    def downloaded(self) -> bool:
        return self.downloaded_format is not None


@dataclass
class VideoExtraFiles:
    video_id: int
    thumbnail: Optional[str] = None
    info_json: Optional[str] = None
    audio_file: Optional[str] = None
    subtitles: Optional[list[str]] = None
    other_files: Optional[dict[str, str]] = None
