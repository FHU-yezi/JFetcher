from datetime import date
from typing import Sequence

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


async def init_db() -> None:
    await COLLECTION.create_indexes(
        [IndexModel(["date", "userSlug"], unique=True)],
    )


async def insert_many(data: Sequence[DailyUpdateRankingRecordDocument]) -> None:
    await COLLECTION.insert_many(x.to_dict() for x in data)
