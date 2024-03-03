from datetime import date
from typing import Optional, Sequence

from jkit._constraints import (
    NonNegativeFloat,
    PositiveFloat,
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

COLLECTION = DB.assets_ranking_records


class UserInfoField(Field, **FIELD_OBJECT_CONFIG):
    id: Optional[PositiveInt]
    slug: Optional[UserSlug]
    name: Optional[UserName]


class AssetsRankingRecordDocument(Documemt, **DOCUMENT_OBJECT_CONFIG):
    date: date
    ranking: PositiveInt

    fp_amount: Optional[NonNegativeFloat]
    ftn_amount: Optional[NonNegativeFloat]
    assets_amount: PositiveFloat

    user_info: UserInfoField


async def init_db() -> None:
    await COLLECTION.create_indexes(
        [IndexModel(["date", "ranking"], unique=True)],
    )


async def insert_many(data: Sequence[AssetsRankingRecordDocument]) -> None:
    await COLLECTION.insert_many(x.to_dict() for x in data)
