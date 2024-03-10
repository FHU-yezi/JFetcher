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


class OldUserField(Field, **FIELD_OBJECT_CONFIG):
    id: Optional[int]
    url: Optional[str]
    name: Optional[str]


class OldAssetsField(Field, **FIELD_OBJECT_CONFIG):
    fp: Optional[float] = field(name="FP")
    ftn: Optional[float] = field(name="FTN")
    total: Optional[float]


class OldAssetsRank(Document, **DOCUMENT_OBJECT_CONFIG):
    date: datetime
    ranking: int

    user: OldUserField
    assets: OldAssetsField

    class Meta:  # type: ignore
        collection = OLD_DB.assets_rank
        indexes: ClassVar[List[IndexModel]] = [
            IndexModel(["date", "ranking"], unique=True),
        ]

