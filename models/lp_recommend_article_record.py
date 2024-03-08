from datetime import date, datetime
from typing import ClassVar, List

from jkit._constraints import (
    ArticleSlug,
    NonEmptyStr,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveInt,
    UserSlug,
)
from msgspec import field
from pymongo import IndexModel

from utils.db import DB
from utils.document_model import (
    DOCUMENT_OBJECT_CONFIG,
    Documemt,
)


class LPRecommendedArticleRecordDocument(Documemt, **DOCUMENT_OBJECT_CONFIG):
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

    author_slug: UserSlug

    class Meta:  # type: ignore
        collection = DB.lp_recommended_article_records
        indexes: ClassVar[List[IndexModel]] = [
            IndexModel(["date", "slug"], unique=True),
        ]

    @classmethod
    async def is_record_exist(cls, slug: str) -> bool:
        return await cls.Meta.collection.find_one({"slug": slug}) is not None
