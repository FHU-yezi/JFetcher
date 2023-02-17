from typing import Optional

from apscheduler.events import JobExecutionEvent

from constants import FetchResult
from utils.log import run_logger
from utils.message import send_task_fail_card, send_task_success_card
from utils.time_helper import human_readable_cost_time


def on_executed_event(event: JobExecutionEvent) -> None:
    task_name: str = event.job_id
    fetch_result, cost_time, data_count = event.retval  # type: ignore

    if fetch_result == FetchResult.SUCCESSED:
        on_task_successed(task_name, cost_time, data_count)
    elif fetch_result == FetchResult.SKIPPED:
        on_task_skipped(task_name)
    elif fetch_result == FetchResult.FAILED:
        on_task_failed(task_name)
    else:
        raise ValueError


def on_error_event(event: JobExecutionEvent) -> None:
    task_name: str = event.job_id
    exception: Exception = event.exception  # type: ignore

    on_task_failed(task_name, exception)


def on_task_successed(task_name: str, cost_time: int, data_count: int) -> None:
    run_logger.debug(
        f"{task_name} 运行成功，采集的数据量：{data_count}，耗时：{cost_time}",
    )

    send_task_success_card(task_name, human_readable_cost_time(cost_time), data_count)


def on_task_failed(task_name: str, exception: Optional[Exception] = None) -> None:
    run_logger.debug(
        f"{task_name} 运行失败，错误信息：{repr(exception)}",
    )

    send_task_fail_card(task_name, repr(exception))


def on_task_skipped(task_name: str) -> None:
    del task_name
    pass
