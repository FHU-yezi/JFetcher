from datetime import datetime
from typing import ClassVar, List

from pymongo import IndexModel

from old_models import OLD_DB
from utils.document_model import (
    DOCUMENT_OBJECT_CONFIG,
    FIELD_OBJECT_CONFIG,
    Document,
    Field,
)


class OldUserField(Field, **FIELD_OBJECT_CONFIG):
    url: str
    name: str


class OldDailyUpdateRank(Document, **DOCUMENT_OBJECT_CONFIG):
    date: datetime
    ranking: int

    user: OldUserField
    days: int

    class Meta:  # type: ignore
        collection = OLD_DB.daily_update_rank
        indexes: ClassVar[List[IndexModel]] = [
            IndexModel(["date", "ranking"]),
        ]
