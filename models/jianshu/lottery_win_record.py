from datetime import datetime

from jkit.msgspec_constraints import (
    NonEmptyStr,
    PositiveInt,
    UserSlug,
)
from sshared.mongo import MODEL_META, Document, Index

from utils.db import JIANSHU_DB


class LotteryWinRecordDocument(Document, **MODEL_META):
    id: PositiveInt
    time: datetime
    award_name: NonEmptyStr

    user_slug: UserSlug

    class Meta:  # type: ignore
        collection = JIANSHU_DB.lottery_win_records
        indexes = (Index(keys=("id",), unique=True),)

    @classmethod
    async def get_latest_record_id(cls) -> int:
        latest_data = await LotteryWinRecordDocument.find_one(sort={"id": "DESC"})
        if not latest_data:
            return 0

        return latest_data.id
