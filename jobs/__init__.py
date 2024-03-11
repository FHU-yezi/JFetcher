from importlib import import_module
from typing import Any, Coroutine, Set, Tuple

from prefect.deployments.runner import RunnerDeployment

DeploymentType = Coroutine[Any, Any, RunnerDeployment]


def import_deployment(path: str) -> DeploymentType:
    module_name, func_name = path.split(":")

    module = import_module(module_name)
    return getattr(module, func_name)


DEPLOYMENT_PATHS: Set[str] = {
    "jobs.jianshu.article_earning_ranking:deployment",
    "jobs.jianshu.assets_ranking:deployment",
    "jobs.jianshu.daily_update_ranking:deployment",
    "jobs.jianshu.lottery:deployment",
    "jobs.jianshu.lp_recommend:deployment",
    "jobs.jpep.ftn_trade:buy_deployment",
    "jobs.jpep.ftn_trade:sell_deployment",
}


DEPLOYMENTS: Tuple[DeploymentType, ...] = tuple(
    import_deployment(x) for x in DEPLOYMENT_PATHS
)
