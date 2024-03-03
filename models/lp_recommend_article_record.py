from datetime import date, datetime
from typing import Sequence

from jkit._constraints import (
    ArticleSlug,
    NonEmptyStr,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveInt,
)
from msgspec import field
from pymongo import IndexModel

from utils.db import DB
from utils.document_model import (
    DOCUMENT_OBJECT_CONFIG,
    FIELD_OBJECT_CONFIG,
    Documemt,
    Field,
)

COLLECTION = DB.lp_recommended_article_records


class AuthorField(Field, **FIELD_OBJECT_CONFIG):
    id: PositiveInt
    slug: str
    name: str


class LPRecommendedArticleRecord(Documemt, **DOCUMENT_OBJECT_CONFIG):
    date: date
    id: PositiveInt
    slug: ArticleSlug
    title: NonEmptyStr
    published_at: datetime

    views_count: NonNegativeInt
    likes_count: NonNegativeInt
    comments_count: NonNegativeInt
    tips_count: NonNegativeFloat
    earned_fp_amount: NonNegativeFloat = field(name="EarnedFPAmount")

    is_paid: bool
    can_comment: bool
    description: str

    author: AuthorField


async def is_record_stored(article_slug: str) -> bool:
    result = await COLLECTION.find_one({"slug": article_slug})

    return result is not None


async def init_db() -> None:
    await COLLECTION.create_indexes([IndexModel(["date", "slug"], unique=True)])


async def insert_many(data: Sequence[LPRecommendedArticleRecord]) -> None:
    await COLLECTION.insert_many(x.to_dict() for x in data)
