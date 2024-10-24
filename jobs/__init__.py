from collections.abc import Coroutine
from importlib import import_module
from typing import Any

from prefect.deployments.runner import RunnerDeployment

from utils.log import logger

DeploymentType = Coroutine[Any, Any, RunnerDeployment]


def import_deployment(path: str) -> DeploymentType:
    module_name, deployment_obj_name = path.split(":")

    logger.debug(
        "已动态导入工作流部署",
        module_name=module_name,
        deployment_obj_name=deployment_obj_name,
    )

    module = import_module(module_name)
    return getattr(module, deployment_obj_name)


DEPLOYMENT_PATHS: set[str] = {
    "jobs.jianshu.article_earning_ranking:deployment",
    "jobs.jianshu.user_assets_ranking:deployment",
    "jobs.jianshu.daily_update_ranking:deployment",
    "jobs.jpep.ftn_trade:buy_deployment",
    "jobs.jpep.ftn_trade:sell_deployment",
}


DEPLOYMENTS: tuple[DeploymentType, ...] = tuple(
    import_deployment(x) for x in DEPLOYMENT_PATHS
)
