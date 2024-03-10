from datetime import datetime
from typing import ClassVar, List, Optional

from msgspec import field
from pymongo import IndexModel

from old_models import OLD_DB
from utils.document_model import (
    DOCUMENT_OBJECT_CONFIG,
    FIELD_OBJECT_CONFIG,
    Document,
    Field,
)


class OldArticleField(Field, **FIELD_OBJECT_CONFIG):
    title: Optional[str]
    url: Optional[str]


class OldAuthorField(Field, **FIELD_OBJECT_CONFIG):
    name: Optional[str] = None
    id: Optional[int] = None
    url: Optional[str] = None


class OldRewardField(Field, **FIELD_OBJECT_CONFIG):
    to_author: float = field(name="to_author")
    to_voter: float = field(name="to_voter")
    total: float


class OldArticleFPRank(Document, **DOCUMENT_OBJECT_CONFIG):
    date: datetime
    ranking: int

    article: OldArticleField
    author: OldAuthorField
    reward: OldRewardField

    class Meta:  # type: ignore
        collection = OLD_DB.article_FP_rank
        indexes: ClassVar[List[IndexModel]] = [
            IndexModel(["date", "ranking"], unique=True),
        ]
