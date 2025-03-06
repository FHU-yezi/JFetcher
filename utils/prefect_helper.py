from prefect.client.schemas.schedules import CronSchedule
from prefect.runtime import flow_run, task_run


def get_cron_schedule(cron: str, /) -> CronSchedule:
    return CronSchedule(cron=cron, timezone="Asia/Shanghai")


def get_task_run_name() -> str:
    task_name = task_run.get_task_name()
    task_run_id_part = task_run.get_id().split("-")[0]  # type: ignore
    return f"{task_name}_{task_run_id_part}"


def get_flow_run_name() -> str:
    return flow_run.get_id().split("-")[0]  # type: ignore
