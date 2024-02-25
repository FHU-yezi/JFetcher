from typing import Any, Dict, Union

from prefect.client.schemas.schedules import CronSchedule

from utils.config import CONFIG
from utils.event_handlers import on_failure_or_crashed


def generate_flow_config(
    name: str,
    retries: int = 1,
    retry_delay_seconds: Union[int, float] = 5,
    timeout: int = 3600,
) -> Dict[str, Any]:
    return {
        "name": name,
        "version": CONFIG.version,
        "retries": retries,
        "retry_delay_seconds": retry_delay_seconds,
        "timeout_seconds": timeout,
        "on_failure": [on_failure_or_crashed],
        "on_crashed": [on_failure_or_crashed],
    }


def generate_deployment_config(name: str, cron: str) -> Dict[str, Any]:
    return {
        "name": f"JFetcher - {name}",
        "version": CONFIG.version,
        "schedule": CronSchedule(
            cron=cron,
            timezone="Asia/Shanghai",
        ),
    }
