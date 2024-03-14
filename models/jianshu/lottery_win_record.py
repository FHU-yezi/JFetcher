from datetime import datetime
from typing import ClassVar, List

from jkit.msgspec_constraints import (
    NonEmptyStr,
    PositiveInt,
    UserSlug,
)
from pymongo import IndexModel

from utils.db import JIANSHU_DB
from utils.document_model import (
    DOCUMENT_OBJECT_CONFIG,
    Document,
)


class LotteryWinRecordDocument(Document, **DOCUMENT_OBJECT_CONFIG):
    id: PositiveInt
    time: datetime
    award_name: NonEmptyStr

    user_slug: UserSlug

    class Meta:  # type: ignore
        collection = JIANSHU_DB.lottery_win_records
        indexes: ClassVar[List[IndexModel]] = [
            IndexModel(["id"], unique=True),
        ]

    @classmethod
    async def get_latest_record_id(cls) -> int:
        latest_data = await LotteryWinRecordDocument.find_one(sort={"id": "DESC"})
        if not latest_data:
            return 0

        return latest_data.id
