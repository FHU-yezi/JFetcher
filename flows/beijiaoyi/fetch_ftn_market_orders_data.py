from collections.abc import AsyncGenerator
from datetime import datetime, timedelta
from typing import Literal

from jkit.beijiaoyi.ftn_macket import FtnMacket, OrderData
from jkit.credentials import BeijiaoyiCredential
from prefect import flow, get_run_logger, task
from prefect.client.schemas.objects import State
from prefect.states import Completed, Failed

from models.beijiaoyi.ftn_macket_record import FTNMacketRecord
from models.beijiaoyi.ftn_order import FTNOrder
from models.beijiaoyi.user import User
from utils.config import CONFIG
from utils.prefect_helper import get_flow_run_name, get_task_run_name

OrdersType = Literal["BUY", "SELL"]


def get_fetch_time() -> datetime:
    time = datetime.now().replace(second=0, microsecond=0)

    # 12:01 -> timedelta = -1 minute
    if time.minute <= 5:  # noqa: PLR2004
        delta = -timedelta(minutes=time.minute % 10)
    # 11:57 -> timedelta = +3 minute
    else:
        delta = timedelta(minutes=10 - (time.minute % 10))

    return time + delta


@task(task_run_name=get_task_run_name)
async def iter_ftn_macket_orders(
    type: OrdersType,
) -> AsyncGenerator[OrderData]:
    async for item in FtnMacket(
        credential=BeijiaoyiCredential.from_bearer_token(CONFIG.beijiaoyi_token)
    ).iter_orders(type=type):
        yield item


@task(task_run_name=get_task_run_name)
async def save_data_to_db(data: list[FTNMacketRecord]) -> None:
    await FTNMacketRecord.insert_many(data)


@flow(
    name="采集简书积分兑换平台简书贝市场挂单数据",
    flow_run_name=get_flow_run_name,
    retries=2,
    retry_delay_seconds=10,
    timeout_seconds=20,
)
async def beijiaoyi_fetch_ftn_market_orders_data(type: OrdersType) -> State:
    logger = get_run_logger()

    if not CONFIG.beijiaoyi_token:
        logger.error("未设置 beijiaoyi_token，无法进行数据采集")
        return Failed()

    time = get_fetch_time()

    data: list[FTNMacketRecord] = []
    async for item in iter_ftn_macket_orders(type):
        user = await User.get_by_id(item.publisher_info.id)
        if not user:
            await User.create(
                id=item.publisher_info.id,
                name=item.publisher_info.name,
                # TODO: 等待 JKit 修复该类型
                avatar_url=item.publisher_info.avatar_url,  # type: ignore
            )
        else:
            await User.update_by_id(
                id=item.publisher_info.id,
                name=item.publisher_info.name,
                # TODO: 等待 JKit 修复该类型
                avatar_url=item.publisher_info.avatar_url,  # type: ignore
            )

        ftn_order = await FTNOrder.get_by_id(item.id)
        if not ftn_order:
            await FTNOrder.create(
                id=item.id,
                type=type,
                publisher_id=item.publisher_info.id,
                publish_time=item.publish_time,
                last_seen_time=time,
            )
        else:
            await FTNOrder.update_by_id(
                id=item.id,
                last_seen_time=time,
            )

        data.append(
            FTNMacketRecord(
                fetch_time=time,
                id=item.id,
                price=item.price,
                total_amount=item.total_amount,
                traded_amount=item.traded_amount,
                remaining_amount=item.tradable_amount,
                minimum_trade_amount=item.minimum_trade_amount,
                maximum_trade_amount=item.maximum_trade_amount,
                completed_trades_count=item.completed_trades_count,
            )
        )

    await save_data_to_db(data)

    return Completed()
