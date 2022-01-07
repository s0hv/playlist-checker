from dataclasses import dataclass

from .DbBase import DbBase


@dataclass
class Tag(DbBase):
    tag: str
