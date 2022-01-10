import os
from typing import Optional, List

import yaml
from pydantic import BaseModel, validator

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
    archive: bool = False


class Config(BaseModel):
    download_sleep_interval: MinMax
    max_downloads_per_run: int
    yt_token: str
    db_conn_string: str
    s3_bucket: str = None
    s3_archive: bool = False  # Must be after bucket
    delete_old_info: bool = True
    delete_old_thumbnail: bool = True

    scripts: List[Script]
    playlists: List[Playlist]
    after: Optional[List[Script]]
    after_all: Optional[List[Script]]

    @validator('s3_archive')
    def validate_s3_bucket(cls, v, values):
        if v and not values.get('s3_bucket', None):
            raise ValueError('S3 bucket name not specified when S3 archive is set on.')
        return v

    @classmethod
    def load(cls):
        yt_token = os.getenv('YT_TOKEN')
        db_conn_string = os.getenv('DATABASE_URL')
        with open('config.yaml', 'r', encoding='utf-8') as f:
            return cls(
                **yaml.load(f, yaml.Loader),
                yt_token=yt_token,
                db_conn_string=db_conn_string,
            )
