from datetime import datetime
from typing import ClassVar, List, Literal, Optional

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


class AmountField(Field, **FIELD_OBJECT_CONFIG):
    total: PositiveInt
    traded: NonNegativeInt
    tradable: NonNegativeInt
    minimum_trade: PositiveInt


class PublisherField(Field, **FIELD_OBJECT_CONFIG):
    is_anonymous: bool
    id: Optional[PositiveInt]
    name: Optional[str]
    hashed_name: Optional[str]
    credit: Optional[NonNegativeInt]


class JPEPFTNTradeOrderDocument(Documemt, **DOCUMENT_OBJECT_CONFIG):
    fetch_time: datetime
    id: PositiveInt
    published_at: datetime
    type: Literal["buy", "sell"]
    price: PositiveFloat
    traded_count: NonNegativeInt

    amount: AmountField
    publisher: PublisherField

    class Settings:  # type: ignore
        collection = COLLECTION
        indexes: ClassVar[List[IndexModel]] = [
            IndexModel(["fetchTime", "id"], unique=True),
        ]
