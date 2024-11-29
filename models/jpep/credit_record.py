from __future__ import annotations

from typing import TYPE_CHECKING

from sshared.postgres import Table

from utils.db import jpep_pool

if TYPE_CHECKING:
    from datetime import datetime

    from sshared.strict_struct import (
        NonNegativeInt,
        PositiveInt,
    )


class CreditRecord(Table, frozen=True):
    time: datetime
    user_id: PositiveInt
    credit: NonNegativeInt

    async def create(self) -> None:
        self.validate()

        async with jpep_pool.get_conn() as conn:
            await conn.execute(
                "INSERT INTO credit_records (time, user_id, credit) VALUES "
                "(%s, %s, %s);",
                (self.time, self.user_id, self.credit),
            )

    @classmethod
    async def get_latest_credit(cls, user_id: int) -> int | None:
        async with jpep_pool.get_conn() as conn:
            cursor = await conn.execute(
                "SELECT credit FROM credit_records WHERE user_id = %s "
                "ORDER BY time DESC LIMIT 1;",
                (user_id,),
            )

            data = await cursor.fetchone()
        if not data:
            return None

        return data[0]
