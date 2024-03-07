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
    Documemt,
)

COLLECTION = DB.daily_update_ranking_records


class DailyUpdateRankingRecordDocument(Documemt, **DOCUMENT_OBJECT_CONFIG):
    date: date
    ranking: PositiveInt
    days: PositiveInt

    user_slug: UserSlug

    class Settings:  # type: ignore
        collection = COLLECTION
        indexes: ClassVar[List[IndexModel]] = [
            IndexModel(["date", "userSlug"], unique=True),
        ]
