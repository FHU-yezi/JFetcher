from datetime import date

from sshared.postgres import Table
from sshared.strict_struct import NonEmptyStr, PositiveInt

from utils.postgres import jianshu_conn


class DailyUpdateRankingRecord(Table, frozen=True):
    date: date
    ranking: PositiveInt
    user_slug: NonEmptyStr
    days: PositiveInt

    @classmethod
    async def _create_table(cls) -> None:
        await jianshu_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_update_ranking_records (
                date DATE NOT NULL,
                ranking SMALLINT NOT NULL,
                user_slug VARCHAR(12),
                days SMALLINT,
                CONSTRAINT pk_daily_update_ranking_records_date_ranking_user_slug PRIMARY KEY (date, ranking, user_slug)
            );
            """  # noqa: E501
        )

    @classmethod
    async def insert_many(cls, data: list["DailyUpdateRankingRecord"]) -> None:
        for item in data:
            item.validate()

        await jianshu_conn.cursor().executemany(
            "INSERT INTO daily_update_ranking_records (date, ranking, "
            "user_slug, days) VALUES (%s, %s, %s, %s);",
            [
                (
                    item.date,
                    item.ranking,
                    item.user_slug,
                    item.days,
                )
                for item in data
            ],
        )
