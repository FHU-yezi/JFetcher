from prefect.client.schemas.schedules import CronSchedule
from prefect.runtime import flow_run, task_run


def get_cron_schedule(cron: str, /) -> CronSchedule:
    return CronSchedule(cron=cron, timezone="Asia/Shanghai")


def get_task_run_name() -> str:
    task_name = task_run.get_task_name()
    task_run_id = task_run.get_id()
    if task_name is None or task_run_id is None:
        raise ValueError

    return f"{task_name}_{task_run_id.split('-')[0]}"


def get_flow_run_name() -> str:
    flow_run_id = flow_run.get_id()
    if flow_run_id is None:
        raise ValueError

    return flow_run_id.split("-")[0]
