from datetime import datetime
from typing import ClassVar, List

from jkit._constraints import (
    NonEmptyStr,
    PositiveInt,
    UserSlug,
)
from pymongo import IndexModel

from utils.db import DB
from utils.document_model import (
    DOCUMENT_OBJECT_CONFIG,
    Document,
)


class JianshuLotteryWinRecordDocument(Document, **DOCUMENT_OBJECT_CONFIG):
    id: PositiveInt
    time: datetime
    award_name: NonEmptyStr

    user_slug: UserSlug

    class Meta:  # type: ignore
        collection = DB.jianshu_lottery_win_records
        indexes: ClassVar[List[IndexModel]] = [
            IndexModel(["id"], unique=True),
        ]

    @classmethod
    async def get_latest_record_id(cls) -> int:
        try:
            latest_data = JianshuLotteryWinRecordDocument.from_dict(
                await cls.Meta.collection.find().sort("id", -1).__anext__()
            )
        except StopAsyncIteration:
            return 0

        return latest_data.id
