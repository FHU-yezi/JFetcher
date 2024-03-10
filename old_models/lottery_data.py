from datetime import datetime
from typing import ClassVar, List

from msgspec import field
from pymongo import IndexModel

from old_models import OLD_DB
from utils.document_model import (
    DOCUMENT_OBJECT_CONFIG,
    FIELD_OBJECT_CONFIG,
    Document,
    Field,
)


class OldUserField(Field, **FIELD_OBJECT_CONFIG):
    id: int
    url: str


class OldLotteryData(Document, **DOCUMENT_OBJECT_CONFIG):
    time: datetime
    reward_name: str = field(name="reward_name")

    user: OldUserField

    class Meta:  # type: ignore
        collection = OLD_DB.lottery_data
        indexes: ClassVar[List[IndexModel]] = [
            IndexModel(["time"]),
        ]
