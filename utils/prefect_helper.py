from prefect.runtime import flow_run, task_run


def get_task_run_name() -> str:
    return task_run.get_task_name()  # type: ignore


def get_flow_run_name() -> str:
    return flow_run.get_id().split("-")[0]  # type: ignore
