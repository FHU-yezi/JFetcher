from datetime import date
from typing import ClassVar, List, Optional

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

    class Settings:  # type: ignore
        collection = COLLECTION
        indexes: ClassVar[List[IndexModel]] = [
            IndexModel(["date", "ranking"], unique=True),
        ]
