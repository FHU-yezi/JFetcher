from datetime import datetime
from typing import Optional

from jkit.msgspec_constraints import NonNegativeInt, PositiveInt
from sshared.mongo import Document, Index

from utils.mongo import JPEP_DB


class CreditHistoryDocument(Document, frozen=True):
    time: datetime
    user_id: PositiveInt
    value: NonNegativeInt

    class Meta:  # type: ignore
        collection = JPEP_DB.credit_history
        indexes = (Index(keys=("time", "userId"), unique=True),)

    @classmethod
    async def get_latest_value(cls, user_id: int) -> Optional[int]:
        latest_record = await cls.find_one({"userId": user_id}, sort={"time": "DESC"})
        if not latest_record:
            return None

        return latest_record.value
