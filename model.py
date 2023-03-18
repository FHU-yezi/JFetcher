from dataclasses import dataclass

from constants import FetchStatus, NoticePolicy


@dataclass
class FetchResult:
    task_name: str
    notice_policy: NoticePolicy
    fetch_status: FetchStatus
    cost_time: int
    data_count: int
