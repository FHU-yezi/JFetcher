from functools import partial
from typing import Any, Dict, Literal, Union

from httpx import AsyncClient, HTTPStatusError
from prefect.client.schemas.objects import Flow, FlowRun, State
from prefect.client.schemas.schedules import CronSchedule

from utils.config import CONFIG
from utils.log import logger

_CLIENT = AsyncClient(http2=True)


async def failed_flow_run_handler(
    status: Literal["Failed", "Crashed"], flow: Flow, flow_run: FlowRun, state: State
) -> None:
    if not CONFIG.feishu_notification.enabled:
        logger.warn("未开启失败飞书通知，跳过消息发送", flow_run_name=flow_run.name)
        return

    response = await _CLIENT.post(
        url=CONFIG.feishu_notification.webhook_url,
        json={
            "msg_type": "interactive",
            "card": {
                "type": "template",
                "data": {
                    "template_id": CONFIG.feishu_notification.failure_card_id,
                    "template_variable": {
                        "status": status,
                        "flow_name": flow.name,
                        "run_name": flow_run.name,
                        "parameters": str(flow_run.parameters),
                        "start_time": flow_run.start_time.in_timezone(
                            "Asia/Shanghai"
                        ).strftime(r"%Y-%m-%d %H:%M:%S")
                        if flow_run.start_time
                        else "[未知]",
                        "retries": flow_run.empirical_policy.retries,
                        "error_message": state.message.replace(
                            "Flow run encountered an exception. ", ""
                        )
                        if state.message
                        else "[未知]",
                        "details_url": f"http://prod-server:4200/flow-runs/flow-run/{flow_run.id}",
                    },
                },
            },
        },
    )

    try:
        response.raise_for_status()
    except HTTPStatusError as e:
        logger.error("发送失败飞书通知时发生异常", exc=e, flow_run_name=flow_run.name)
    else:
        logger.info("发送失败飞书通知成功", flow_run_name=flow_run.name)


def generate_flow_config(
    name: str,
    retries: int = 1,
    retry_delay_seconds: Union[int, float] = 5,
    timeout: int = 3600,
) -> Dict[str, Any]:
    return {
        "name": name,
        "version": None,
        "retries": retries,
        "retry_delay_seconds": retry_delay_seconds,
        "timeout_seconds": timeout,
        "on_failure": [partial(failed_flow_run_handler, status="Failed")],
        "on_crashed": [partial(failed_flow_run_handler, status="Crashed")],
    }


def generate_deployment_config(name: str, cron: str) -> Dict[str, Any]:
    return {
        "name": f"JFetcher - {name}",
        "version": None,
        "schedule": CronSchedule(
            cron=cron,
            timezone="Asia/Shanghai",
        ),
    }
