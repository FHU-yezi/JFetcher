from functools import wraps
from typing import Any, Callable

from httpx import ConnectError, TimeoutException
from sspeedup.retry import exponential_backoff_policy, retry

from utils.log import run_logger


def retry_on_network_error(func: Callable) -> Callable:
    @retry(
        exponential_backoff_policy(),
        (ConnectError, TimeoutException),
        max_tries=5,
        on_retry=lambda event: run_logger.warning(
            f"函数 {event.func.__name__} 发生超时重试，尝试次数：{event.tries}，"
            f"等待时间：{round(event.wait, 3)}"
        )
    )
    @wraps(func)
    def inner(*args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    return inner
