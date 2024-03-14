from datetime import datetime
from typing import ClassVar, List, Optional

from jkit.msgspec_constraints import NonNegativeInt, PositiveInt
from pymongo import IndexModel

from utils.db import JPEP_DB
from utils.document_model import (
    DOCUMENT_OBJECT_CONFIG,
    Document,
)


class CreditHistoryDocument(Document, **DOCUMENT_OBJECT_CONFIG):
    time: datetime
    user_id: PositiveInt
    value: NonNegativeInt

    class Meta:  # type: ignore
        collection = JPEP_DB.credit_history
        indexes: ClassVar[List[IndexModel]] = [
            IndexModel(["time", "userId"], unique=True),
        ]

    @classmethod
    async def get_latest_value(cls, user_id: int) -> Optional[int]:
        latest_record = await cls.find_one({"userId": user_id}, sort={"time": "DESC"})
        if not latest_record:
            return None

        return latest_record.value
