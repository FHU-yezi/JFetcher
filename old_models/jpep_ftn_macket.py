from datetime import datetime
from typing import Literal, Optional

from msgspec import field
from sshared.mongo import MODEL_META, Document, Field, Index

from old_models import OLD_DB


class OldAmountField(Field, **MODEL_META):
    total: int
    traded: int
    remaining: int
    tradable: int


class OldUserField(Field, **MODEL_META):
    id: int
    name: str
    name_md5: Optional[str] = field(name="name_md5")


class OldJPEPFTNMacket(Document, **MODEL_META):
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
        indexes = (Index(keys=("fetch_time",)),)
