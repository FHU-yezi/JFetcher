from functools import wraps

from backoff import expo, on_exception
from httpx import TimeoutException

from utils.log import run_logger


def retry_on_timeout(func):
    @on_exception(
        expo,
        TimeoutException,
        base=2,
        factor=4,
        max_tries=5,
        on_backoff=lambda details: run_logger.warning(
            f"函数 {func.__name__} 发生超时重试，尝试次数：{details['tries']}，"
            f"等待时间：{round(details['wait'], 3)}"  # type: ignore
        ),
    )
    @wraps(func)
    def inner(*args, **kwargs):
        return func(*args, **kwargs)

    return inner
