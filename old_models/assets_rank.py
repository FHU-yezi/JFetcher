from datetime import datetime
from typing import Optional

from msgspec import field
from sshared.mongo import MODEL_META, Document, Field, Index

from old_models import OLD_DB


class OldUserField(Field, **MODEL_META):
    id: Optional[int]
    url: Optional[str]
    name: Optional[str]


class OldAssetsField(Field, **MODEL_META):
    fp: Optional[float] = field(name="FP")
    ftn: Optional[float] = field(name="FTN")
    total: Optional[float]


class OldAssetsRank(Document, **MODEL_META):
    date: datetime
    ranking: int

    user: OldUserField
    assets: OldAssetsField

    class Meta:  # type: ignore
        collection = OLD_DB.assets_rank
        indexes = (Index(keys=("date", "ranking"), unique=True),)
