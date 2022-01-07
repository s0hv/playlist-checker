from dataclasses import dataclass
from typing import Optional

from src.enum import Site
from .DbBase import DbBase


@dataclass
class Playlist(DbBase):
    name: str
    playlist_id: str
    site: Site


@dataclass
class PlaylistVideo:
    playlist_id: Optional[int] = None
    video_id: Optional[int] = None
