from __future__ import annotations

from datetime import date

from sshared.postgres import Table
from sshared.strict_struct import NonEmptyStr, PositiveInt

from utils.db import jianshu_pool


class DailyUpdateRankingRecord(Table, frozen=True):
    date: date
    ranking: PositiveInt
    slug: NonEmptyStr
    days: PositiveInt

    @classmethod
    async def create(cls, *, date: date, ranking: int, slug: str, days: int) -> None:
        async with jianshu_pool.get_conn() as conn:
            await conn.cursor().execute(
                "INSERT INTO daily_update_ranking_records (date, ranking, "
                "slug, days) VALUES (%s, %s, %s, %s);",
                (
                    date,
                    ranking,
                    slug,
                    days,
                ),
            )

    @classmethod
    async def count_by_date(cls, date: date, /) -> int:
        async with jianshu_pool.get_conn() as conn:
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM daily_update_ranking_records WHERE date = %s;",
                (date,),
            )

            data = await cursor.fetchone()
            if not data:
                raise ValueError

        return data[0]
