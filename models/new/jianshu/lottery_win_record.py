from datetime import datetime

from sshared.postgres import Table
from sshared.strict_struct import NonEmptyStr, PositiveInt

from utils.postgres import get_jianshu_conn


class LotteryWinRecord(Table, frozen=True):
    id: PositiveInt
    time: datetime
    user_slug: NonEmptyStr
    award_name: NonEmptyStr

    @classmethod
    async def _create_table(cls) -> None:
        conn = await get_jianshu_conn()
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lottery_win_records (
                id INTEGER CONSTRAINT pk_lottery_win_records_id PRIMARY KEY,
                time TIMESTAMP NOT NULL,
                user_slug VARCHAR(12),
                award_name TEXT NOT NULL
            );
            """
        )

    @classmethod
    async def insert_many(cls, data: list["LotteryWinRecord"]) -> None:
        for item in data:
            item.validate()

        conn = await get_jianshu_conn()
        await conn.cursor().executemany(
            "INSERT INTO lottery_win_records (id, time, user_slug, "
            "award_name) VALUES (%s, %s, %s, %s);",
            [
                (
                    item.id,
                    item.time,
                    item.user_slug,
                    item.award_name,
                )
                for item in data
            ],
        )
