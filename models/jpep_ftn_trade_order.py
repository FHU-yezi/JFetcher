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


class AmountField(Field, **FIELD_OBJECT_CONFIG):
    total: PositiveInt
    traded: NonNegativeInt
    tradable: NonNegativeInt
    minimum_trade: PositiveInt


class JPEPFTNTradeOrderDocument(Documemt, **DOCUMENT_OBJECT_CONFIG):
    fetch_time: datetime
    id: PositiveInt
    published_at: datetime
    type: Literal["buy", "sell"]
    price: PositiveFloat
    traded_count: NonNegativeInt

    amount: AmountField
    publisher_id: Optional[PositiveInt]

    class Meta:  # type: ignore
        collection = DB.jpep_ftn_trade_orders
        indexes: ClassVar[List[IndexModel]] = [
            IndexModel(["fetchTime", "id"], unique=True),
        ]
