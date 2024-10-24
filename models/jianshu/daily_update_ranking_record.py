from datetime import datetime

from jkit.msgspec_constraints import (
    PositiveInt,
    UserSlug,
)
from sshared.mongo import Document, Index

from utils.mongo import JIANSHU_DB


class DailyUpdateRankingRecordDocument(Document, frozen=True):
    date: datetime
    ranking: PositiveInt
    days: PositiveInt

    user_slug: UserSlug

    class Meta:  # type: ignore
        collection = JIANSHU_DB.daily_update_ranking_records
        indexes = (Index(keys=("date", "userSlug"), unique=True),)
