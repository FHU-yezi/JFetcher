from datetime import datetime
from typing import Literal, Optional, Sequence

from jkit._constraints import (
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
)
from pymongo import IndexModel

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


async def init_db() -> None:
    await COLLECTION.create_indexes(
        [IndexModel(["fetchTime", "orderId"], unique=True)],
    )


async def insert_many(data: Sequence[JPEPFTNTradeOrderDocument]) -> None:
    await COLLECTION.insert_many(x.to_dict() for x in data)
