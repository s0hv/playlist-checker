from enum import Enum, IntEnum


class ScriptDataField(Enum):
    new = 'new'
    deleted = 'deleted'
    new_deleted = 'new_deleted'


class Site(IntEnum):
    YouTube = 0


class S3ObjectType(Enum):
    video = 'video'
    thumbnail = 'thumbnail'
    subtitle = 'subtitle'
    metadata = 'metadata'
    other = 'other'
