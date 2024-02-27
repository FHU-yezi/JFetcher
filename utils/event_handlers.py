from typing import Literal

from httpx import AsyncClient
from prefect.client.schemas.objects import Flow, FlowRun, State

from utils.config import CONFIG

CLIENT = AsyncClient(http2=True)


async def on_failure_or_crashed(
    status: Literal["Failed", "Crashed"], flow: Flow, flow_run: FlowRun, state: State
) -> None:
    if not CONFIG.feishu_notification.enabled:
        return

    response = await CLIENT.post(
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
                        "start_time": flow_run.start_time.strftime(r"%Y-%m-%d %H:%M:%S")
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

    response.raise_for_status()
