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
    async def insert_many(cls, data: list[UserAssetsRankingRecord]) -> None:
        for item in data:
            item.validate()

        async with jianshu_pool.get_conn() as conn:
            await conn.cursor().executemany(
                "INSERT INTO user_assets_ranking_records (date, ranking, "
                "slug, fp, ftn, assets) VALUES (%s, %s, %s, %s, %s, %s);",
                [
                    (
                        item.date,
                        item.ranking,
                        item.slug,
                        item.fp,
                        item.ftn,
                        item.assets,
                    )
                    for item in data
                ],
            )
