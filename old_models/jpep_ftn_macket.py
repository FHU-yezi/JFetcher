from datetime import datetime
from typing import ClassVar, List, Literal

from msgspec import field
from pymongo import IndexModel

from old_models import OLD_DB
from utils.document_model import (
    DOCUMENT_OBJECT_CONFIG,
    FIELD_OBJECT_CONFIG,
    Document,
    Field,
)


class OldAmountField(Field, **FIELD_OBJECT_CONFIG):
    total: int
    traded: int
    remaining: int
    tradable: int


class OldUserField(Field, **FIELD_OBJECT_CONFIG):
    id: int
    name: str
    name_md5: str = field(name="name_md5")


class OldJPEPFTNMacket(Document, **DOCUMENT_OBJECT_CONFIG):
    fetch_time: datetime = field(name="fetch_time")
    order_id: int = field(name="order_id")
    trade_type: Literal["buy", "sell"] = field(name="trade_type")
    publish_time: datetime = field(name="publish_time")
    is_anonymous: bool = field(name="is_anonymous")
    price: float

    amount: OldAmountField
    traded_percentage: float = field(name="traded_percentage")
    minimum_trade_count: int = field(name="minimum_trade_count")
    transactions_count: int = field(name="transactions_count")
    user: OldUserField

    class Meta:  # type: ignore
        collection = OLD_DB.JPEP_FTN_macket
        indexes: ClassVar[List[IndexModel]] = [
            IndexModel(["fetch_time"]),
        ]
