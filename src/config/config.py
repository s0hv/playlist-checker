import os
from typing import Optional, List

import yaml
from pydantic import BaseModel

from src.enum.enums import ScriptDataField, Site


class MinMax(BaseModel):
    min: int
    max: int


class Script(BaseModel):
    name: str
    required_fields: Optional[List[ScriptDataField]]
    script: str


class Playlist(BaseModel):
    name: str
    playlist_id: str
    site: Site
    after: Optional[List[Script]]


class Config(BaseModel):
    download_sleep_interval: MinMax
    max_downloads_per_run: int
    yt_token: str

    scripts: List[Script]
    playlists: List[Playlist]
    after: Optional[List[Script]]
    after_all: Optional[List[Script]]

    @classmethod
    def load(cls):
        yt_token = os.getenv('YT_TOKEN')
        with open('config.yaml', 'r', encoding='utf-8') as f:
            return cls(
                yt_token=yt_token,
                **yaml.load(f, yaml.Loader)
            )

    @property
    def db_conn_string(self) -> str:
        return os.getenv('DATABASE_URL')
