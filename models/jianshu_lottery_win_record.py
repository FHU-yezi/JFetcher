from datetime import datetime
from typing import Sequence

from jkit._constraints import (
    NonEmptyStr,
    PositiveInt,
    UserName,
    UserSlug,
)
from pymongo import IndexModel

from utils.db import DB
from utils.document_model import (
    DOCUMENT_OBJECT_CONFIG,
    FIELD_OBJECT_CONFIG,
    Documemt,
    Field,
)

COLLECTION = DB.jianshu_lottery_win_records


class UserInfoField(Field, **FIELD_OBJECT_CONFIG):
    id: PositiveInt
    slug: UserSlug
    name: UserName


class JianshuLotteryWinRecordDocument(Documemt, **DOCUMENT_OBJECT_CONFIG):
    record_id: PositiveInt
    time: datetime
    award_name: NonEmptyStr
    user_info: UserInfoField


async def get_latest_record_id() -> int:
    try:
        latest_data = JianshuLotteryWinRecordDocument.from_dict(
            await COLLECTION.find().sort("recordId", -1).__anext__()
        )
    except StopAsyncIteration:
        return 0

    return latest_data.record_id


async def init_db() -> None:
    await COLLECTION.create_indexes(
        [IndexModel(["recordId"], unique=True)],
    )


async def insert_many(data: Sequence[JianshuLotteryWinRecordDocument]) -> None:
    await COLLECTION.insert_many(x.to_dict() for x in data)
