from datetime import date

from sshared.postgres import Table
from sshared.strict_struct import NonEmptyStr, PositiveInt

from utils.postgres import get_jianshu_conn


class DailyUpdateRankingRecord(Table, frozen=True):
    date: date
    ranking: PositiveInt
    slug: NonEmptyStr
    days: PositiveInt

    @classmethod
    async def _create_table(cls) -> None:
        conn = await get_jianshu_conn()
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_update_ranking_records (
                date DATE NOT NULL,
                ranking SMALLINT NOT NULL,
                slug VARCHAR(12),
                days SMALLINT,
                CONSTRAINT pk_daily_update_ranking_records_date_slug PRIMARY KEY (date, slug)
            );
            """  # noqa: E501
        )

    @classmethod
    async def insert_many(cls, data: list["DailyUpdateRankingRecord"]) -> None:
        for item in data:
            item.validate()

        conn = await get_jianshu_conn()
        await conn.cursor().executemany(
            "INSERT INTO daily_update_ranking_records (date, ranking, "
            "slug, days) VALUES (%s, %s, %s, %s);",
            [
                (
                    item.date,
                    item.ranking,
                    item.slug,
                    item.days,
                )
                for item in data
            ],
        )
