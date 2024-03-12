from datetime import date, datetime
from typing import ClassVar, List

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
    id: int
    url: str
    title: str
    release_time: datetime = field(name="release_time")

    views_count: int = field(name="views_count")
    likes_count: int = field(name="likes_count")
    comments_count: int = field(name="comments_count")
    rewards_count: int = field(name="rewards_count")
    total_fp_amount: float = field(name="total_FP_amount")

    is_paid: bool = field(name="is_paid")
    is_commentable: bool = field(name="is_commentable")
    summary: str


class OldAuthorField(Field, **FIELD_OBJECT_CONFIG):
    id: int
    url: str
    name: str


class OldLPCollections(Document, **DOCUMENT_OBJECT_CONFIG):
    fetch_date: date = field(name="fetch_date")
    from_collection: str = field(name="from_collection")

    article: OldArticleField
    author: OldAuthorField

    class Meta:  # type: ignore
        collection = OLD_DB.LP_collections
        indexes: ClassVar[List[IndexModel]] = [
            IndexModel(["fetch_date"]),
        ]
