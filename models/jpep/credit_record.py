from __future__ import annotations

from datetime import datetime

from sshared.postgres import Table
from sshared.strict_struct import (
    NonNegativeInt,
    PositiveInt,
)

from utils.db import jpep_pool


class CreditRecord(Table, frozen=True):
    time: datetime
    user_id: PositiveInt
    credit: NonNegativeInt

    @classmethod
    async def create(cls, *, time: datetime, user_id: int, credit: int) -> None:
        async with jpep_pool.get_conn() as conn:
            await conn.execute(
                "INSERT INTO credit_records (time, user_id, credit) VALUES "
                "(%s, %s, %s);",
                (time, user_id, credit),
            )

    @classmethod
    async def get_by_user_id(cls, user_id: int) -> CreditRecord | None:
        async with jpep_pool.get_conn() as conn:
            cursor = await conn.execute(
                "SELECT time, credit FROM credit_records WHERE user_id = %s "
                "ORDER BY time DESC LIMIT 1;",
                (user_id,),
            )

            data = await cursor.fetchone()
        if not data:
            return None

        return cls(
            time=data[0],
            user_id=user_id,
            credit=data[1],
        ).validate()
