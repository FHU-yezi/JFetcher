from datetime import datetime

from msgspec import field
from sshared.mongo import MODEL_META, Document, Field, Index

from old_models import OLD_DB


class OldUserField(Field, **MODEL_META):
    id: int
    url: str
    name: str


class OldLotteryData(Document, **MODEL_META):
    _id: int
    time: datetime
    reward_name: str = field(name="reward_name")

    user: OldUserField

    class Meta:  # type: ignore
        collection = OLD_DB.lottery_data
        indexes = (Index(keys=("time",)),)
