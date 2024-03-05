from datetime import date
from typing import Optional, Sequence

from jkit._constraints import (
    ArticleSlug,
    NonEmptyStr,
    PositiveFloat,
    PositiveInt,
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

COLLECTION = DB.article_earning_ranking_records


class ArticleField(Field, **FIELD_OBJECT_CONFIG):
    slug: Optional[ArticleSlug]
    title: Optional[NonEmptyStr]


class EarningField(Field, **FIELD_OBJECT_CONFIG):
    to_author: PositiveFloat
    to_voter: PositiveFloat


class ArticleEarningRankingRecordDocument(Documemt, **DOCUMENT_OBJECT_CONFIG):
    date: date
    ranking: PositiveInt

    article: ArticleField
    author_slug: Optional[UserSlug]
    earning: EarningField


async def init_db() -> None:
    await COLLECTION.create_indexes(
        [IndexModel(["date", "ranking"], unique=True)],
    )


async def insert_many(data: Sequence[ArticleEarningRankingRecordDocument]) -> None:
    await COLLECTION.insert_many(x.to_dict() for x in data)
