from datetime import date
from typing import Optional

from sshared.postgres import Table
from sshared.strict_struct import NonEmptyStr, NonNegativeFloat, PositiveInt

from utils.postgres import jianshu_conn


class AssetsRankingRecord(Table, frozen=True):
    date: date
    ranking: PositiveInt
    user_slug: Optional[NonEmptyStr]

    fp: Optional[NonNegativeFloat]
    ftn: Optional[NonNegativeFloat]
    assets: Optional[NonNegativeFloat]

    @classmethod
    async def _create_table(cls) -> None:
        await jianshu_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS assets_ranking_records (
                date DATE NOT NULL,
                ranking SMALLINT NOT NULL,
                user_slug VARCHAR(12),
                fp NUMERIC,
                ftn NUMERIC,
                assets NUMERIC,
                CONSTRAINT pk_assets_ranking_records_date_ranking PRIMARY KEY (date, ranking)
            );
            """  # noqa: E501
        )

    @classmethod
    async def insert_many(cls, data: list["AssetsRankingRecord"]) -> None:
        for item in data:
            item.validate()

        await jianshu_conn.cursor().executemany(
            "INSERT INTO assets_ranking_records (date, ranking, "
            "user_slug, fp, ftn, assets) VALUES (%s, %s, %s, %s, %s, %s);",
            [
                (
                    item.date,
                    item.ranking,
                    item.user_slug,
                    item.fp,
                    item.ftn,
                    item.assets,
                )
                for item in data
            ],
        )
