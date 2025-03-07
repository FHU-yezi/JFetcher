from collections.abc import AsyncGenerator
from datetime import datetime, timedelta
from typing import Literal

from jkit.jpep.ftn_macket import FtnMacket, OrderData
from prefect import flow, task
from prefect.client.schemas.objects import State
from prefect.states import Completed

from models.jpep.credit_record import CreditRecord
from models.jpep.ftn_macket_record import FTNMacketRecord
from models.jpep.ftn_order import FTNOrder
from models.jpep.user import User
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
    async for item in FtnMacket().iter_orders(type=type):
        yield item


@task(task_run_name=get_task_run_name)
async def save_data_to_db(data: list[FTNMacketRecord]) -> None:
    await FTNMacketRecord.insert_many(data)


@flow(
    name="采集贝交易平台简书贝市场挂单数据",
    flow_run_name=get_flow_run_name,
    retries=2,
    retry_delay_seconds=10,
    timeout_seconds=20,
)
async def jpep_fetch_ftn_market_orders_data(type: OrdersType) -> State:
    time = get_fetch_time()

    data: list[FTNMacketRecord] = []
    async for item in iter_ftn_macket_orders(type):
        user = await User.get_by_id(item.publisher_info.id)
        if not user:
            await User.create(
                id=item.publisher_info.id,
                name=item.publisher_info.name,
                hashed_name=item.publisher_info.hashed_name,
                avatar_url=item.publisher_info.avatar_url,
            )
        else:
            await User.update_by_id(
                id=item.publisher_info.id,
                name=item.publisher_info.name,
                hashed_name=item.publisher_info.hashed_name,
                avatar_url=item.publisher_info.avatar_url,
            )

        credit_record = await CreditRecord.get_by_user_id(item.publisher_info.id)
        # 如果信用值记录不存在或已更新，创建新的信用值记录
        if not credit_record or item.publisher_info.credit != credit_record.credit:
            await CreditRecord.create(
                time=time,
                user_id=item.publisher_info.id,
                credit=item.publisher_info.credit,
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
            await FTNOrder.update_by_id(id=item.id, last_seen_time=time)

        data.append(
            FTNMacketRecord(
                fetch_time=time,
                id=item.id,
                price=item.price,
                total_amount=item.total_amount,
                traded_amount=item.traded_amount,
                remaining_amount=item.tradable_amount,
                minimum_trade_amount=item.minimum_trade_amount,
                completed_trades_count=item.completed_trades_count,
            )
        )

    await save_data_to_db(data)

    return Completed()
