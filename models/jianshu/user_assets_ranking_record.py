from __future__ import annotations

from datetime import date

from sshared.postgres import Table
from sshared.strict_struct import NonEmptyStr, NonNegativeFloat, PositiveInt

from utils.db import jianshu_pool


class UserAssetsRankingRecord(Table, frozen=True):
    date: date
    ranking: PositiveInt
    slug: NonEmptyStr | None

    fp: NonNegativeFloat | None
    ftn: NonNegativeFloat | None
    assets: NonNegativeFloat | None

    @classmethod
    async def create(
        cls,
        *,
        date: date,
        ranking: int,
        slug: str | None,
        fp: float | None,
        ftn: float | None,
        assets: float | None,
    ) -> None:
        async with jianshu_pool.get_conn() as conn:
            await conn.cursor().execute(
                "INSERT INTO user_assets_ranking_records (date, ranking, "
                "slug, fp, ftn, assets) VALUES (%s, %s, %s, %s, %s, %s);",
                (
                    date,
                    ranking,
                    slug,
                    fp,
                    ftn,
                    assets,
                ),
            )

    @classmethod
    async def count_by_date(cls, date: date, /) -> int:
        async with jianshu_pool.get_conn() as conn:
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM user_assets_ranking_records WHERE date = %s;",
                (date,),
            )

            data = await cursor.fetchone()
            if not data:
                raise ValueError

        return data[0]
