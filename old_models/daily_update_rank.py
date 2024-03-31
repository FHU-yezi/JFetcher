from datetime import datetime

from sshared.mongo import MODEL_META, Document, Field, Index

from old_models import OLD_DB


class OldUserField(Field, **MODEL_META):
    url: str
    name: str


class OldDailyUpdateRank(Document, **MODEL_META):
    date: datetime
    ranking: int

    user: OldUserField
    days: int

    class Meta:  # type: ignore
        collection = OLD_DB.daily_update_rank
        indexes = (Index(keys=("date", "ranking")),)
