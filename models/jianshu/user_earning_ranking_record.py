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

    @classmethod
    async def insert_many(cls, data: list[UserEarningRankingRecord], /) -> None:
        for item in data:
            item.validate()

        async with jianshu_pool.get_conn() as conn:
            await conn.cursor().executemany(
                "INSERT INTO user_earning_ranking_records (date, type, "
                "ranking, slug, total_earning, creating_earning, voting_earning) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s);",
                [
                    (
                        item.date,
                        item.type,
                        item.ranking,
                        item.slug,
                        item.total_earning,
                        item.creating_earning,
                        item.voting_earning,
                    )
                    for item in data
                ],
            )
