from __future__ import annotations

from datetime import date

from sshared.postgres import Table
from sshared.strict_struct import PositiveInt

from utils.db import jianshu_pool


class UsersCountRecord(Table, frozen=True):
    date: date
    total_users_count: PositiveInt

    async def create(self) -> None:
        self.validate()

        async with jianshu_pool.get_conn() as conn:
            await conn.execute(
                "INSERT INTO users_count_records (date, total_users_count) "
                "VALUES (%s, %s);",
                (self.date, self.total_users_count),
            )

    @classmethod
    async def is_records_exist(cls, date: date) -> bool:
        async with jianshu_pool.get_conn() as conn:
            cursor = await conn.execute(
                "SELECT 1 FROM users_count_records WHERE date = %s LIMIT 1;",
                (date,),
            )

            return await cursor.fetchone() is not None
