from datetime import datetime
from typing import Optional

from msgspec import field
from sshared.mongo import MODEL_META, Document, Field, Index

from old_models import OLD_DB


class OldArticleField(Field, **MODEL_META):
    title: Optional[str]
    url: Optional[str]


class OldAuthorField(Field, **MODEL_META):
    name: Optional[str] = None
    id: Optional[int] = None
    url: Optional[str] = None


class OldRewardField(Field, **MODEL_META):
    to_author: float = field(name="to_author")
    to_voter: float = field(name="to_voter")
    total: float


class OldArticleFPRank(Document, **MODEL_META):
    date: datetime
    ranking: int

    article: OldArticleField
    author: OldAuthorField
    reward: OldRewardField

    class Meta:  # type: ignore
        collection = OLD_DB.article_FP_rank
        indexes = (Index(keys=("date", "ranking"), unique=True),)
