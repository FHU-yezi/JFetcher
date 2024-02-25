from importlib import import_module
from typing import Any, Coroutine, Set, Tuple

from prefect.deployments.runner import RunnerDeployment

DeploymentType = Coroutine[Any, Any, RunnerDeployment]


def import_deployment(path: str) -> DeploymentType:
    module_name, func_name = path.split(":")

    module = import_module(module_name)
    return getattr(module, func_name)


DEPLOYMENT_PATHS: Set[str] = {
    "jobs.fetch_article_earning_ranking_records:deployment",
    "jobs.fetch_assets_ranking_records:deployment",
    "jobs.fetch_daily_update_ranking_records:deployment",
    "jobs.fetch_jianshu_lottery_win_records:deployment",
    "jobs.fetch_jpep_ftn_trade_orders:buy_deployment",
    "jobs.fetch_jpep_ftn_trade_orders:sell_deployment",
}


DEPLOYMENTS: Tuple[DeploymentType, ...] = tuple(
    import_deployment(x) for x in DEPLOYMENT_PATHS
)
