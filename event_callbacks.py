from typing import Optional

from apscheduler.events import JobExecutionEvent

from constants import FetchStatus
from model import FetchResult
from utils.log import run_logger
from utils.message import send_task_fail_card, send_task_success_card
from utils.time_helper import human_readable_cost_time


def on_executed_event(event: JobExecutionEvent) -> None:
    fetch_result: FetchResult = event.retval  # type: ignore

    if fetch_result.fetch_status == FetchStatus.SUCCESSED:
        on_task_successed(fetch_result)
    elif fetch_result.fetch_status == FetchStatus.SKIPPED:
        on_task_skipped(fetch_result)
    elif fetch_result.fetch_status == FetchStatus.FAILED:
        task_name: str = event.job_id
        on_task_failed(task_name)
    else:
        raise ValueError


def on_error_event(event: JobExecutionEvent) -> None:
    task_name: str = event.job_id
    exception: Exception = event.exception  # type: ignore

    on_task_failed(task_name, exception)


def on_task_successed(fetch_result: FetchResult) -> None:
    run_logger.info(
        "采集任务运行成功",
        task_name=fetch_result.task_name,
        data_count=fetch_result.data_count,
        cost_time=fetch_result.cost_time,
    )

    send_task_success_card(
        fetch_result.task_name,
        human_readable_cost_time(fetch_result.cost_time),
        fetch_result.data_count,
    )


def on_task_failed(task_name: str, exception: Optional[Exception] = None) -> None:
    run_logger.error("采集任务运行失败", task_name=task_name, exception=exception)

    send_task_fail_card(task_name, repr(exception))


def on_task_skipped(fetch_result: FetchResult) -> None:
    run_logger.info(
        "采集任务被跳过",
        task_name=fetch_result.task_name,
    )
