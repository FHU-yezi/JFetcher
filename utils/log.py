from prefect import runtime
from sshared.logging import Logger
from sshared.logging.types import ExtraType

from utils.config import CONFIG

logger = Logger(
    display_level=CONFIG.logging.display_level,
    save_level=CONFIG.logging.save_level,
    connection_string=CONFIG.logging.connection_string,
    table=CONFIG.logging.table,
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


def log_flow_run_success(logger: Logger, **kwargs: ExtraType) -> None:
    logger.info(
        "工作流执行成功",
        flow_run_name=runtime.flow_run.name,  # type: ignore
        **kwargs,
    )
