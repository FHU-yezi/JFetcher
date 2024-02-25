from datetime import datetime
from typing import List, Literal, Optional

from bson import ObjectId
from jkit._constraints import (
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
)
from jkit.jpep.ftn_macket import FTNMacket, FTNMacketOrderRecord
from prefect import flow
from prefect.states import Completed, State

from utils.config_generators import generate_deployment_config, generate_flow_config
from utils.db import DB
from utils.document_model import (
    DOCUMENT_OBJECT_CONFIG,
    FIELD_OBJECT_CONFIG,
    Documemt,
    Field,
)

COLLECTION = DB.jpep_ftn_trade_orders


class PublisherInfoField(Field, **FIELD_OBJECT_CONFIG):
    is_anonymous: bool
    id: Optional[PositiveInt]
    name: Optional[str]
    hashed_name: Optional[str]
    credit: Optional[NonNegativeInt]


class JPEPFTNTradeOrderDocument(Documemt, **DOCUMENT_OBJECT_CONFIG):
    fetch_time: datetime
    order_id: PositiveInt
    type: Literal["buy", "sell"]
    price: PositiveFloat

    total_amount: PositiveInt
    traded_amount: NonNegativeInt
    tradable_amount: NonNegativeInt
    minimum_trade_amount: PositiveInt

    traded_count: NonNegativeInt
    publish_time: datetime

    publisher_info: PublisherInfoField


def get_fetch_time() -> datetime:
    current_dt = datetime.now()

    # 保证采集时间对齐 10 分钟间隔
    return current_dt.replace(minute=current_dt.minute // 10, second=0, microsecond=0)


def process_item(
    item: FTNMacketOrderRecord,
    /,
    *,
    fetch_time: datetime,
    type: Literal["buy", "sell"],  # noqa: A002
) -> JPEPFTNTradeOrderDocument:
    return JPEPFTNTradeOrderDocument(
        _id=ObjectId(),
        type=type,
        fetch_time=fetch_time,
        order_id=item.id,
        price=item.price,
        total_amount=item.total_amount,
        traded_amount=item.traded_amount,
        tradable_amount=item.tradable_amount,
        minimum_trade_amount=item.minimum_trade_amount,
        traded_count=item.traded_count,
        publish_time=item.publish_time,
        publisher_info=PublisherInfoField(
            is_anonymous=item.publisher_info.is_anonymous,
            id=item.publisher_info.id,
            name=item.publisher_info.name,
            hashed_name=item.publisher_info.hashed_name,
            credit=item.publisher_info.credit,
        ),
    )


@flow(
    **generate_flow_config(
        name="采集简书积分兑换平台简书贝交易挂单",
    ),
)
async def flow_func(type: Literal["buy", "sell"]) -> State:  # noqa: A002
    fetch_time = get_fetch_time()

    data: List[JPEPFTNTradeOrderDocument] = []
    async for item in FTNMacket().iter_orders(type=type):
        processed_item = process_item(item, fetch_time=fetch_time, type=type)
        data.append(processed_item)

    await COLLECTION.insert_many(x.to_dict() for x in data)

    return Completed(message=f"fetch_time={fetch_time}, data_count={len(data)}")


buy_deployment = flow_func.to_deployment(
    parameters={"type": "buy"},
    **generate_deployment_config(
        name="采集简书积分兑换平台简书贝交易买单",
        cron="*/10 * * * *",
    ),
)

sell_deployment = flow_func.to_deployment(
    parameters={"type": "sell"},
    **generate_deployment_config(
        name="采集简书积分兑换平台简书贝交易卖单",
        cron="*/10 * * * *",
    ),
)
