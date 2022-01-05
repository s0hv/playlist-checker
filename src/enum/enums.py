from enum import Enum, IntEnum


class ScriptDataField(Enum):
    new = 'new'
    deleted = 'deleted'
    new_deleted = 'new_deleted'


class Site(IntEnum):
    Youtube = 0
