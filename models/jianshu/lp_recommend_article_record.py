from datetime import datetime

from jkit.msgspec_constraints import (
    ArticleSlug,
    NonEmptyStr,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveInt,
    UserSlug,
)
from msgspec import field
from sshared.mongo import Document, Index

from utils.db import JIANSHU_DB


class LPRecommendedArticleRecordDocument(Document, frozen=True):
    date: datetime
    id: PositiveInt
    slug: ArticleSlug
    title: NonEmptyStr
    published_at: datetime

    views_count: NonNegativeInt
    likes_count: NonNegativeInt
    comments_count: NonNegativeInt
    tips_count: NonNegativeFloat
    earned_fp_amount: NonNegativeFloat = field(name="earnedFPAmount")

    is_paid: bool
    can_comment: bool
    description: str

    author_slug: UserSlug

    class Meta:  # type: ignore
        collection = JIANSHU_DB.lp_recommended_article_records
        indexes = (Index(keys=("date", "slug"), unique=True),)

    @classmethod
    async def is_record_exist(cls, slug: str) -> bool:
        return await cls.find_one({"slug": slug}) is not None
