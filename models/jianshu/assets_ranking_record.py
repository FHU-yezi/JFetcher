from datetime import date
from typing import ClassVar, List, Optional

from jkit._constraints import (
    NonNegativeFloat,
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


class AmountField(Field, **FIELD_OBJECT_CONFIG):
    fp: Optional[NonNegativeFloat]
    ftn: Optional[NonNegativeFloat]
    assets: Optional[PositiveFloat]


class AssetsRankingRecordDocument(Document, **DOCUMENT_OBJECT_CONFIG):
    date: date
    ranking: PositiveInt

    amount: AmountField
    user_slug: Optional[UserSlug]

    class Meta:  # type: ignore
        collection = JIANSHU_DB.assets_ranking_records
        indexes: ClassVar[List[IndexModel]] = [
            IndexModel(["date", "ranking"], unique=True),
        ]
