from datetime import datetime, timedelta
from typing import List, Literal

from jkit.jpep.ftn_macket import FTNMacket, FTNMacketOrderRecord
from prefect import flow

from models.jpep.credit_history import CreditHistoryDocument
from models.jpep.ftn_trade_order import (
    AmountField,
    FTNTradeOrderDocument,
)
from models.jpep.user import UserDocument
from utils.config_generators import (
    generate_deployment_config,
    generate_flow_config,
)
from utils.log import log_flow_run_start, log_flow_run_success, logger


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
    /,
    *,
    fetch_time: datetime,
    type: Literal["buy", "sell"],  # noqa: A002
) -> FTNTradeOrderDocument:
    if item.publisher_info.id:
        await UserDocument.insert_or_update_one(
            updated_at=fetch_time,
            id=item.publisher_info.id,
            name=item.publisher_info.name,
            hashed_name=item.publisher_info.hashed_name,
            avatar_url=item.publisher_info.avatar_url,
        )

        latest_credit_value = await CreditHistoryDocument.get_latest_value(
            item.publisher_info.id
        )
        if not latest_credit_value or latest_credit_value != item.publisher_info.credit:
            await CreditHistoryDocument(
                time=fetch_time,
                user_id=item.publisher_info.id,
                value=item.publisher_info.credit,
            ).save()

    return FTNTradeOrderDocument(
        fetch_time=fetch_time,
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
    ).validate()


@flow(
    **generate_flow_config(
        name="采集简书积分兑换平台简书贝交易挂单",
    ),
)
async def main(type: Literal["buy", "sell"]) -> None:  # noqa: A002
    log_flow_run_start(logger)

    fetch_time = get_fetch_time()

    data: List[FTNTradeOrderDocument] = []
    async for item in FTNMacket().iter_orders(type=type):
        processed_item = await process_item(item, fetch_time=fetch_time, type=type)
        data.append(processed_item)

    await FTNTradeOrderDocument.insert_many(data)

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
