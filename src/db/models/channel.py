from dataclasses import dataclass
from typing import Optional

from src.db.models import DbBase
from src.enum import Site


@dataclass
class Channel(DbBase):
    channel_id: str
    site: Optional[Site] = None
    name: Optional[str] = None
    thumbnail: Optional[str] = None

