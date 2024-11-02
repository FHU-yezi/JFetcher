from datetime import datetime, timedelta
from typing import Literal

from jkit.jpep.ftn_macket import FTNMacket, FTNMacketOrderRecord
from prefect import flow

from models.jpep.credit_record import CreditRecord
from models.jpep.ftn_macket_record import FTNMacketRecord
from models.jpep.ftn_order import FTNOrder, TypeEnum
from models.jpep.user import User
from utils.log import log_flow_run_start, log_flow_run_success, logger
from utils.prefect_helper import (
    generate_deployment_config,
    generate_flow_config,
)


def get_fetch_time() -> datetime:
    dt = datetime.now()

    # 保证采集时间对齐 10 分钟间隔
    discard = timedelta(
        minutes=dt.minute % 10,
        seconds=dt.second,
        microseconds=dt.microsecond,
    )
    dt -= discard
    if discard >= timedelta(minutes=5):
        dt += timedelta(minutes=10)
    return dt


async def process_item(
    item: FTNMacketOrderRecord,
    time: datetime,
    type: Literal["buy", "sell"],  # noqa: A002
) -> FTNMacketRecord:
    if item.publisher_info.id:
        await User.upsert(
            id=item.publisher_info.id,
            name=item.publisher_info.name,
            hashed_name=item.publisher_info.hashed_name,
            avatar_url=item.publisher_info.avatar_url,
        )

        latest_credit = await CreditRecord.get_latest_credit(item.publisher_info.id)
        # 如果没有记录过这个用户的信用值，或信用值已修改，增加新的记录
        if not latest_credit or latest_credit != item.publisher_info.credit:
            await CreditRecord(
                time=time,
                user_id=item.publisher_info.id,
                credit=item.publisher_info.credit,
            ).create()

    order = await FTNOrder.get_by_id(item.id)
    if not order:
        await FTNOrder(
            id=item.id,
            type={"buy": TypeEnum.BUY, "sell": TypeEnum.SELL}[type],
            publisher_id=item.publisher_info.id,
            publish_time=item.publish_time,
            last_seen_time=time,
        ).create()
    else:
        await FTNOrder.update_last_seen_time(order.id, time)

    return FTNMacketRecord(
        fetch_time=time,
        id=item.id,
        price=item.price,
        traded_count=item.traded_count,
        total_amount=item.total_amount,
        traded_amount=item.traded_amount,
        remaining_amount=item.tradable_amount,
        minimum_trade_amount=item.minimum_trade_amount,
    )


@flow(
    **generate_flow_config(
        name="采集简书积分兑换平台简书贝交易挂单",
    ),
)
async def main(type: Literal["buy", "sell"]) -> None:  # noqa: A002
    log_flow_run_start(logger)

    fetch_time = get_fetch_time()

    data: list[FTNMacketRecord] = []
    async for item in FTNMacket().iter_orders(type=type):
        processed_item = await process_item(item, time=fetch_time, type=type)
        data.append(processed_item)

    if data:
        await FTNMacketRecord.insert_many(data)
    else:
        logger.warn("没有可采集的挂单信息，跳过数据写入")

    log_flow_run_success(logger, data_count=len(data))


buy_deployment = main.to_deployment(
    parameters={"type": "buy"},
    **generate_deployment_config(
        name="采集简书积分兑换平台简书贝交易买单",
        cron="*/10 * * * *",
    ),
)

sell_deployment = main.to_deployment(
    parameters={"type": "sell"},
    **generate_deployment_config(
        name="采集简书积分兑换平台简书贝交易卖单",
        cron="*/10 * * * *",
    ),
)
