from __future__ import annotations

from datetime import date
from typing import Literal

from sshared.postgres import Table
from sshared.strict_struct import NonEmptyStr, NonNegativeFloat, PositiveInt

from utils.db import jianshu_pool

UserEarningRankingRecordType = Literal["ALL", "CREATING", "VOTING"]


class UserEarningRankingRecord(Table, frozen=True):
    date: date
    type: UserEarningRankingRecordType
    ranking: PositiveInt
    slug: NonEmptyStr

    total_earning: NonNegativeFloat
    creating_earning: NonNegativeFloat
    voting_earning: NonNegativeFloat

    @classmethod
    async def create(  # noqa: PLR0913
        cls,
        *,
        date: date,
        type: UserEarningRankingRecordType,
        ranking: int,
        slug: str,
        total_earning: float,
        creating_earning: float,
        voting_earning: float,
    ) -> None:
        async with jianshu_pool.get_conn() as conn:
            await conn.cursor().execute(
                "INSERT INTO user_earning_ranking_records (date, type, "
                "ranking, slug, total_earning, creating_earning, voting_earning) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s);",
                (
                    date,
                    type,
                    ranking,
                    slug,
                    total_earning,
                    creating_earning,
                    voting_earning,
                ),
            )

    @classmethod
    async def count_by_date(cls, date: date, /) -> int:
        async with jianshu_pool.get_conn() as conn:
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM user_earning_ranking_records WHERE date = %s;",
                (date,),
            )

            data = await cursor.fetchone()
            if not data:
                raise ValueError

        return data[0]
