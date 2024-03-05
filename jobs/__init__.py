from importlib import import_module
from typing import Any, Coroutine, Set, Tuple

from prefect.deployments.runner import RunnerDeployment

DeploymentType = Coroutine[Any, Any, RunnerDeployment]


def import_deployment(path: str) -> DeploymentType:
    module_name, func_name = path.split(":")

    module = import_module(module_name)
    return getattr(module, func_name)


DEPLOYMENT_PATHS: Set[str] = {
    "jobs.article_earning_ranking:deployment",
    "jobs.assets_ranking:deployment",
    "jobs.daily_update_ranking:deployment",
    "jobs.jianshu_lottery:deployment",
    "jobs.jpep_ftn_trade:buy_deployment",
    "jobs.jpep_ftn_trade:sell_deployment",
    "jobs.lp_recommended_articles:deployment",
}


DEPLOYMENTS: Tuple[DeploymentType, ...] = tuple(
    import_deployment(x) for x in DEPLOYMENT_PATHS
)
