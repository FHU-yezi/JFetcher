from datetime import datetime

from msgspec import field
from sshared.mongo import MODEL_META, Document, Field, Index

from old_models import OLD_DB


class OldArticleField(Field, **MODEL_META):
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


class OldAuthorField(Field, **MODEL_META):
    id: int
    url: str
    name: str


class OldLPCollections(Document, **MODEL_META):
    fetch_date: datetime = field(name="fetch_date")
    from_collection: str = field(name="from_collection")

    article: OldArticleField
    author: OldAuthorField

    class Meta:  # type: ignore
        collection = OLD_DB.LP_collections
        indexes = (Index(keys=("fetch_date",)),)
