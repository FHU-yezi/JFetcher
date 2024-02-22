from typing import Any, Coroutine, Tuple

from prefect import Flow
from prefect.deployments.runner import RunnerDeployment

from jobs.fetch_article_earning_rank_records import (
    fetch_article_earning_rank_records_job,
)
from utils.job_model import Job

FlowType = Flow[[], Coroutine[Any, Any, None]]
DeploymentType = Coroutine[Any, Any, RunnerDeployment]


def create_flow(job: Job) -> FlowType:
    job.func.name = job.name
    job.func.version = job.version

    job.func.retries = job.retries
    job.func.retry_delay_seconds = job.retry_delay
    job.func.timeout_seconds = job.timeout

    return job.func


def create_deployment(job: Job, flow: FlowType) -> DeploymentType:
    return flow.to_deployment(
        name=f"JFetcher - {flow.name}",
        cron=job.cron,
        version=job.version,
    )


JOBS: Tuple[Job, ...] = (fetch_article_earning_rank_records_job,)

FLOWS: Tuple[FlowType, ...] = tuple(map(create_flow, JOBS))
DEPLOYMENTS: Tuple[DeploymentType, ...] = tuple(
    (create_deployment(job, flow) for job, flow in zip(JOBS, FLOWS))
)
