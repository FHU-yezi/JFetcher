from datetime import datetime
from typing import Optional

from jkit.msgspec_constraints import (
    NonNegativeFloat,
    PositiveFloat,
    PositiveInt,
    UserSlug,
)
from sshared.mongo import MODEL_META, Document, Field, Index

from utils.db import JIANSHU_DB


class AmountField(Field, **MODEL_META):
    fp: Optional[NonNegativeFloat]
    ftn: Optional[NonNegativeFloat]
    assets: Optional[PositiveFloat]


class AssetsRankingRecordDocument(Document, **MODEL_META):
    date: datetime
    ranking: PositiveInt

    amount: AmountField
    user_slug: Optional[UserSlug]

    class Meta:  # type: ignore
        collection = JIANSHU_DB.assets_ranking_records
        indexes = (Index(keys=("date", "ranking"), unique=True),)
