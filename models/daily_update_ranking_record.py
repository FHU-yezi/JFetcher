from datetime import date
from typing import ClassVar, List

from jkit._constraints import (
    PositiveInt,
    UserSlug,
)
from pymongo import IndexModel

from utils.db import DB
from utils.document_model import (
    DOCUMENT_OBJECT_CONFIG,
    Document,
)


class DailyUpdateRankingRecordDocument(Document, **DOCUMENT_OBJECT_CONFIG):
    date: date
    ranking: PositiveInt
    days: PositiveInt

    user_slug: UserSlug

    class Meta:  # type: ignore
        collection = DB.daily_update_ranking_records
        indexes: ClassVar[List[IndexModel]] = [
            IndexModel(["date", "userSlug"], unique=True),
        ]
