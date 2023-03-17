from dataclasses import dataclass

from constants import FetchStatus


@dataclass
class FetchResult:
    task_name: str
    fetch_status: FetchStatus
    cost_time: int
    data_count: int
