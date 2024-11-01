from datetime import datetime, timedelta
from typing import Literal

from jkit.jpep.ftn_macket import FTNMacket, FTNMacketOrderRecord
from prefect import flow

from models.jpep.credit_record import CreditRecord
from models.jpep.ftn_trade_order import AmountField, FTNTradeOrderDocument
from models.jpep.new.ftn_macket_record import FTNMacketRecord
from models.jpep.new.ftn_order import FTNOrder, TypeEnum
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
) -> FTNTradeOrderDocument:
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

    return FTNTradeOrderDocument(
        fetch_time=time,
        id=item.id,
        published_at=item.publish_time,
        type=type,
        price=item.price,
        traded_count=item.traded_count,
        amount=AmountField(
            total=item.total_amount,
            traded=item.traded_amount,
            tradable=item.tradable_amount,
            minimum_trade=item.minimum_trade_amount,
        ),
        publisher_id=item.publisher_info.id,
    )


async def transform_and_write_new_data_source(
    type: Literal["buy", "sell"],  # noqa: A002
    old_data: list[FTNTradeOrderDocument],
) -> None:
    new_data: list[FTNMacketRecord] = []
    for item in old_data:
        order = await FTNOrder.get_by_id(item.id)
        if not order:
            await FTNOrder(
                id=item.id,
                type={"buy": TypeEnum.BUY, "sell": TypeEnum.SELL}[type],
                publisher_id=item.publisher_id,
                publish_time=item.published_at,
                last_seen_time=item.fetch_time,
            ).create()
        else:
            await FTNOrder.update_last_seen_time(order.id, item.fetch_time)

        new_data.append(
            FTNMacketRecord(
                fetch_time=item.fetch_time,
                id=item.id,
                price=item.price,
                traded_count=item.traded_count,
                total_amount=item.amount.total,
                traded_amount=item.amount.traded,
                remaining_amount=item.amount.tradable,
                minimum_trade_amount=item.amount.minimum_trade,
            )
        )

    await FTNMacketRecord.insert_many(new_data)


@flow(
    **generate_flow_config(
        name="采集简书积分兑换平台简书贝交易挂单",
    ),
)
async def main(type: Literal["buy", "sell"]) -> None:  # noqa: A002
    log_flow_run_start(logger)

    fetch_time = get_fetch_time()

    data: list[FTNTradeOrderDocument] = []
    async for item in FTNMacket().iter_orders(type=type):
        processed_item = await process_item(item, time=fetch_time, type=type)
        data.append(processed_item)

    if data:
        await FTNTradeOrderDocument.insert_many(data)
        await transform_and_write_new_data_source(type, data)
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
