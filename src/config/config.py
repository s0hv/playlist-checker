import os
from typing import Optional, List

import yaml
from pydantic import BaseModel, field_validator
from pydantic_core.core_schema import ValidationInfo

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
    after: Optional[List[Script]] = None
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
    delete_local_files: bool = True

    filename_template: str = '%s'

    scripts: List[Script]
    playlists: List[Playlist]
    after: Optional[List[Script]] = None
    after_all: Optional[List[Script]] = None

    @field_validator('s3_archive')
    def validate_s3_bucket(cls, v: str, values: ValidationInfo):
        if v and not values.data.get('s3_bucket', None):
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
