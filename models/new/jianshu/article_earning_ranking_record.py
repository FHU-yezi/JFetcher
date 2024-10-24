from datetime import date
from typing import Optional

from sshared.postgres import Table
from sshared.strict_struct import NonEmptyStr, PositiveFloat, PositiveInt

from utils.postgres import jianshu_conn


class ArticleEarningRankingRecord(Table, frozen=True):
    date: date
    ranking: PositiveInt
    slug: Optional[NonEmptyStr]
    title: Optional[NonEmptyStr]
    author_slug: Optional[NonEmptyStr]

    author_earning: PositiveFloat
    voter_earning: PositiveFloat

    @classmethod
    async def _create_table(cls) -> None:
        await jianshu_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS article_earning_ranking_records (
                date DATE NOT NULL,
                ranking SMALLINT NOT NULL,
                slug VARCHAR(12),
                title TEXT,
                author_slug VARCHAR(12),
                author_earning NUMERIC NOT NULL,
                voter_earning NUMERIC NOT NULL,
                CONSTRAINT pk_article_earning_ranking_records_date_ranking PRIMARY KEY (date, ranking)
            );
            """  # noqa: E501
        )

    @classmethod
    async def insert_many(cls, data: list["ArticleEarningRankingRecord"]) -> None:
        for item in data:
            item.validate()

        await jianshu_conn.cursor().executemany(
            "INSERT INTO article_earning_ranking_records (date, ranking, "
            "slug, title, author_slug, author_earning, voter_earning) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s);",
            [
                (
                    item.date,
                    item.ranking,
                    item.slug,
                    item.title,
                    item.author_slug,
                    item.author_earning,
                    item.voter_earning,
                )
                for item in data
            ],
        )
