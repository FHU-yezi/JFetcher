from datetime import datetime

from jkit.msgspec_constraints import (
    PositiveInt,
    UserSlug,
)
from sshared.mongo import MODEL_META, Document, Index

from utils.db import JIANSHU_DB


class DailyUpdateRankingRecordDocument(Document, **MODEL_META):
    date: datetime
    ranking: PositiveInt
    days: PositiveInt

    user_slug: UserSlug

    class Meta:  # type: ignore
        collection = JIANSHU_DB.daily_update_ranking_records
        indexes = (Index(keys=("date", "userSlug"), unique=True),)
