from datetime import datetime
from typing import Sequence

from jkit._constraints import (
    NonEmptyStr,
    PositiveInt,
    UserSlug,
)
from pymongo import IndexModel

from utils.db import DB
from utils.document_model import (
    DOCUMENT_OBJECT_CONFIG,
    Documemt,
)

COLLECTION = DB.jianshu_lottery_win_records


class JianshuLotteryWinRecordDocument(Documemt, **DOCUMENT_OBJECT_CONFIG):
    id: PositiveInt
    time: datetime
    award_name: NonEmptyStr

    user_slug: UserSlug


async def get_latest_record_id() -> int:
    try:
        latest_data = JianshuLotteryWinRecordDocument.from_dict(
            await COLLECTION.find().sort("id", -1).__anext__()
        )
    except StopAsyncIteration:
        return 0

    return latest_data.id


async def init_db() -> None:
    await COLLECTION.create_indexes(
        [IndexModel(["id"], unique=True)],
    )


async def insert_many(data: Sequence[JianshuLotteryWinRecordDocument]) -> None:
    await COLLECTION.insert_many(x.to_dict() for x in data)
