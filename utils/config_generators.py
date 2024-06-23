from functools import partial
from typing import Any, Dict, Union

from prefect.client.schemas.schedules import CronSchedule

from utils.event_handlers import on_failure_or_crashed


def generate_flow_config(
    name: str,
    retries: int = 1,
    retry_delay_seconds: Union[int, float] = 5,
    timeout: int = 3600,
) -> Dict[str, Any]:
    return {
        "name": name,
        "version": None,
        "retries": retries,
        "retry_delay_seconds": retry_delay_seconds,
        "timeout_seconds": timeout,
        "on_failure": [partial(on_failure_or_crashed, status="Failed")],
        "on_crashed": [partial(on_failure_or_crashed, status="Crashed")],
    }


def generate_deployment_config(name: str, cron: str) -> Dict[str, Any]:
    return {
        "name": f"JFetcher - {name}",
        "version": None,
        "schedule": CronSchedule(
            cron=cron,
            timezone="Asia/Shanghai",
        ),
    }