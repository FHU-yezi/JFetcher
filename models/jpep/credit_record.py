from datetime import datetime
from typing import Optional

from sshared.postgres import Table
from sshared.strict_struct import (
    NonNegativeInt,
    PositiveInt,
)

from utils.postgres import get_jpep_conn


class CreditRecord(Table, frozen=True):
    time: datetime
    user_id: PositiveInt
    credit: NonNegativeInt

    @classmethod
    async def _create_table(cls) -> None:
        conn = await get_jpep_conn()
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS credit_records (
                time TIMESTAMP NOT NULL,
                user_id INTEGER NOT NULL,
                credit INTEGER NOT NULL,
                CONSTRAINT pk_credit_records_time_user_id PRIMARY KEY (time, user_id)
            );
            """
        )

    async def create(self) -> None:
        self.validate()

        conn = await get_jpep_conn()
        await conn.execute(
            "INSERT INTO credit_records (time, user_id, credit) VALUES "
            "(%s, %s, %s);",
            (self.time, self.user_id, self.credit),
        )

    @classmethod
    async def get_latest_credit(cls, user_id: int) -> Optional[int]:
        conn = await get_jpep_conn()
        cursor = await conn.execute(
            "SELECT credit FROM credit_records WHERE user_id = %s "
            "ORDER BY time DESC LIMIT 1;",
            (user_id,),
        )

        data = await cursor.fetchone()
        if not data:
            return None

        return data[0]
