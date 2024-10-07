from functools import partial
from typing import Any, Literal, Union

from prefect.client.schemas.objects import Flow, FlowRun, State
from prefect.client.schemas.schedules import CronSchedule
from sshared.notifier import Notifier

from utils.config import CONFIG
from utils.log import logger

if CONFIG.notify.enabled:
    notifier = Notifier(
        host=CONFIG.notify.host, port=CONFIG.notify.port, token=CONFIG.notify.token
    )
else:
    notifier = None


async def failed_flow_run_handler(
    status: Literal["Failed", "Crashed"], flow: Flow, flow_run: FlowRun, state: State
) -> None:
    if not notifier:
        logger.warn("未开启 Gotify 通知，跳过失败通知发送", flow_run_name=flow_run.name)
        return

    start_time = (
        flow_run.start_time.in_timezone("Asia/Shanghai").strftime(r"%Y-%m-%d %H:%M:%S")
        if flow_run.start_time
        else "[未知]",
    )
    error_message = (
        state.message.replace("Flow run encountered an exception. ", "")
        if state.message
        else "[未知]"
    )

    await notifier.send_message(
        title=f"工作流 {flow.name} 运行失败（{status}）",
        message=f"""
运行名称：{flow_run.name}\n
参数：{flow_run.parameters!s}\n
开始时间：{start_time}\n
重试次数：{flow_run.empirical_policy.retries}\n
错误信息：{error_message}\n\n
[在 Prefect 中查看](http://prod-server:4200/flow-runs/flow-run/{flow_run.id}/)
        """,
        markdown=True,
    )

    logger.info("发送失败通知成功", flow_run_name=flow_run.name)


def generate_flow_config(
    name: str,
    retries: int = 1,
    retry_delay_seconds: Union[int, float] = 5,
    timeout: int = 3600,
) -> dict[str, Any]:
    return {
        "name": name,
        "version": None,
        "retries": retries,
        "retry_delay_seconds": retry_delay_seconds,
        "timeout_seconds": timeout,
        "on_failure": [partial(failed_flow_run_handler, status="Failed")],
        "on_crashed": [partial(failed_flow_run_handler, status="Crashed")],
    }


def generate_deployment_config(name: str, cron: str) -> dict[str, Any]:
    return {
        "name": f"JFetcher - {name}",
        "version": None,
        "schedule": CronSchedule(
            cron=cron,
            timezone="Asia/Shanghai",
        ),
    }
