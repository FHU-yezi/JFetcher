from datetime import date
from typing import Sequence

from jkit._constraints import (
    PositiveInt,
    UserName,
    UserSlug,
)
from pymongo import IndexModel

from utils.db import DB
from utils.document_model import (
    DOCUMENT_OBJECT_CONFIG,
    FIELD_OBJECT_CONFIG,
    Documemt,
    Field,
)

COLLECTION = DB.daily_update_ranking_records


class UserField(Field, **FIELD_OBJECT_CONFIG):
    slug: UserSlug
    name: UserName


class DailyUpdateRankingRecordDocument(Documemt, **DOCUMENT_OBJECT_CONFIG):
    date: date
    ranking: PositiveInt
    days: PositiveInt

    user: UserField


async def init_db() -> None:
    await COLLECTION.create_indexes(
        [IndexModel(["date", "user.slug"], unique=True)],
    )


async def insert_many(data: Sequence[DailyUpdateRankingRecordDocument]) -> None:
    await COLLECTION.insert_many(x.to_dict() for x in data)
