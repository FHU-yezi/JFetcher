from datetime import date
from typing import Optional

from sshared.postgres import Table
from sshared.strict_struct import NonEmptyStr, NonNegativeFloat, PositiveInt

from utils.postgres import get_jianshu_conn


class UserAssetsRankingRecord(Table, frozen=True):
    date: date
    ranking: PositiveInt
    slug: Optional[NonEmptyStr]

    fp: Optional[NonNegativeFloat]
    ftn: Optional[NonNegativeFloat]
    assets: Optional[NonNegativeFloat]

    @classmethod
    async def _create_table(cls) -> None:
        conn = await get_jianshu_conn()
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_assets_ranking_records (
                date DATE NOT NULL,
                ranking SMALLINT NOT NULL,
                slug VARCHAR(12),
                fp NUMERIC,
                ftn NUMERIC,
                assets NUMERIC,
                CONSTRAINT pk_user_assets_ranking_records_date_ranking PRIMARY KEY (date, ranking)
            );
            """  # noqa: E501
        )

    @classmethod
    async def insert_many(cls, data: list["UserAssetsRankingRecord"]) -> None:
        for item in data:
            item.validate()

        conn = await get_jianshu_conn()
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
