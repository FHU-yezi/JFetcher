from datetime import datetime
from typing import Optional

from jkit.msgspec_constraints import (
    NonNegativeFloat,
    PositiveFloat,
    PositiveInt,
    UserSlug,
)
from sshared.mongo import Document, Field, Index

from utils.mongo import JIANSHU_DB


class AmountField(Field, frozen=True):
    fp: Optional[NonNegativeFloat]
    ftn: Optional[NonNegativeFloat]
    assets: Optional[PositiveFloat]


class AssetsRankingRecordDocument(Document, frozen=True):
    date: datetime
    ranking: PositiveInt

    amount: AmountField
    user_slug: Optional[UserSlug]

    class Meta:  # type: ignore
        collection = JIANSHU_DB.assets_ranking_records
        indexes = (Index(keys=("date", "ranking"), unique=True),)
