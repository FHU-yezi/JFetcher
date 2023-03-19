from enum import IntEnum


class FetchStatus(IntEnum):
    FAILED = -1
    SUCCESSED = 0
    SKIPPED = 1


class NoticePolicy(IntEnum):
    DISABLE = -1
    ALWAYS = 0
    ONLY_FAILED_SKIPPED = 1
    ONLY_FAILED = 2
