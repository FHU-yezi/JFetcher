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


class AmountField(Field, **FIELD_OBJECT_CONFIG):
    fp: Optional[NonNegativeFloat]
    ftn: Optional[NonNegativeFloat]
    assets: PositiveFloat


class UserField(Field, **FIELD_OBJECT_CONFIG):
    id: Optional[PositiveInt]
    slug: Optional[UserSlug]
    name: Optional[UserName]


class AssetsRankingRecordDocument(Documemt, **DOCUMENT_OBJECT_CONFIG):
    date: date
    ranking: PositiveInt

    amount: AmountField
    user: UserField


async def init_db() -> None:
    await COLLECTION.create_indexes(
        [IndexModel(["date", "ranking"], unique=True)],
    )


async def insert_many(data: Sequence[AssetsRankingRecordDocument]) -> None:
    await COLLECTION.insert_many(x.to_dict() for x in data)
