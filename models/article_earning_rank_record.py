from typing import Optional

from beanie import Document
from pydantic import BaseModel, Field, PastDate, PositiveFloat, PositiveInt


class ArticleField(BaseModel):
    title: Optional[str]
    slug: Optional[str]


class AuthorField(BaseModel):
    id: Optional[PositiveInt]
    slug: Optional[str]
    name: Optional[str]


class EarningField(BaseModel):
    to_author: PositiveFloat = Field(serialization_alias="toAuthor")
    to_voter: PositiveFloat = Field(serialization_alias="toVoter")


class ArticleEarningRankRecordModel(Document):
    date: PastDate
    ranking: PositiveInt
    article: ArticleField
    author: AuthorField
    earning: EarningField

    class Settings:
        name = "article_earning_rank_record"
        indexes = ("date", "ranking")
