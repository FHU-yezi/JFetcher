from typing import Any, Callable, Coroutine, List, Optional, Union

from msgspec import Struct
from prefect import Flow, State
from prefect.tasks import exponential_backoff

from utils.config import CONFIG


class Job(Struct):
    func: Flow[[], Coroutine[Any, Any, State]]

    name: str
    version: str = CONFIG.version
    cron: Optional[str] = None

    retries: int = 3
    retry_delay: Union[
        int,
        float,
        List[Union[int, float]],
        Callable[[int], List[Union[int, float]]],
    ] = exponential_backoff(10)
    timeout: int = 3600
