from datetime import date
from typing import ClassVar, List

from jkit._constraints import (
    PositiveInt,
    UserSlug,
)
from pymongo import IndexModel

from utils.db import JIANSHU_DB
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
        collection = JIANSHU_DB.daily_update_ranking_records
        indexes: ClassVar[List[IndexModel]] = [
            IndexModel(["date", "userSlug"], unique=True),
        ]
