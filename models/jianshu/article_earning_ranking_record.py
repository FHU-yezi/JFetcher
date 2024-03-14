from datetime import datetime
from typing import ClassVar, List, Optional

from jkit.msgspec_constraints import (
    ArticleSlug,
    NonEmptyStr,
    PositiveFloat,
    PositiveInt,
    UserSlug,
)
from pymongo import IndexModel

from utils.db import JIANSHU_DB
from utils.document_model import (
    DOCUMENT_OBJECT_CONFIG,
    FIELD_OBJECT_CONFIG,
    Document,
    Field,
)


class ArticleField(Field, **FIELD_OBJECT_CONFIG):
    slug: Optional[ArticleSlug]
    title: Optional[NonEmptyStr]


class EarningField(Field, **FIELD_OBJECT_CONFIG):
    to_author: PositiveFloat
    to_voter: PositiveFloat


class ArticleEarningRankingRecordDocument(Document, **DOCUMENT_OBJECT_CONFIG):
    date: datetime
    ranking: PositiveInt

    article: ArticleField
    author_slug: Optional[UserSlug]
    earning: EarningField

    class Meta:  # type: ignore
        collection = JIANSHU_DB.article_earning_ranking_records
        indexes: ClassVar[List[IndexModel]] = [
            IndexModel(["date", "ranking"], unique=True),
        ]
