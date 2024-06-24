from prefect import runtime
from pymongo import MongoClient
from sshared.logging import Logger, _ExtraType

from utils.config import CONFIG

logger = Logger(
    # TODO
    save_level=CONFIG.logging.save_level,
    display_level=CONFIG.logging.display_level,
    save_collection=MongoClient()[CONFIG.mongodb.database].log
    if CONFIG.logging.enable_save
    else None,
)


def get_flow_run_name() -> str:
    return runtime.flow_run.name  # type: ignore


def log_flow_run_start(logger: Logger) -> str:
    logger.info(
        "开始执行工作流",
        flow_run_name=runtime.flow_run.name,  # type: ignore
        flow_name=runtime.flow_run.flow_name,  # type: ignore
        deployment_name=runtime.deployment.name,  # type: ignore
    )

    # 返回流程运行名称，供后续日志使用
    return get_flow_run_name()  # type: ignore


def log_flow_run_success(logger: Logger, **kwargs: _ExtraType) -> None:
    logger.info(
        "工作流执行成功",
        flow_run_name=runtime.flow_run.name,  # type: ignore
        **kwargs,
    )
