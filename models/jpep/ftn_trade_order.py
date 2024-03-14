from datetime import datetime
from typing import ClassVar, List, Literal

from jkit.msgspec_constraints import (
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
)
from pymongo import IndexModel

from utils.db import JPEP_DB
from utils.document_model import (
    DOCUMENT_OBJECT_CONFIG,
    FIELD_OBJECT_CONFIG,
    Document,
    Field,
)


class AmountField(Field, **FIELD_OBJECT_CONFIG):
    total: PositiveInt
    traded: NonNegativeInt
    tradable: NonNegativeInt
    minimum_trade: PositiveInt


class FTNTradeOrderDocument(Document, **DOCUMENT_OBJECT_CONFIG):
    fetch_time: datetime
    id: PositiveInt
    published_at: datetime
    type: Literal["buy", "sell"]
    price: PositiveFloat
    traded_count: NonNegativeInt

    amount: AmountField
    publisher_id: PositiveInt

    class Meta:  # type: ignore
        collection = JPEP_DB.ftn_trade_orders
        indexes: ClassVar[List[IndexModel]] = [
            IndexModel(["fetchTime", "id"], unique=True),
        ]
