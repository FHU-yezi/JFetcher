from asyncio import sleep
from typing import Any, Callable


def async_retry(*, attempts: int = 3, delay: float = 5.0) -> Callable:
    def outer(func: Callable) -> Callable:
        async def inner(*args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
            for i in range(attempts):
                try:
                    return await func(*args, **kwargs)
                except Exception:  # noqa: PERF203, BLE001
                    if i == attempts - 1:
                        raise
                    await sleep(delay)
            return None

        return inner

    return outer
