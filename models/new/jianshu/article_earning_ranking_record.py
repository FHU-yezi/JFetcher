from datetime import date
from typing import Optional

from sshared.postgres import Table
from sshared.strict_struct import NonEmptyStr, PositiveFloat, PositiveInt

from utils.postgres import jianshu_conn


class ArticleEarningRankingRecord(Table, frozen=True):
    date: date
    ranking: PositiveInt
    article_slug: Optional[NonEmptyStr]
    article_title: Optional[NonEmptyStr]
    author_slug: Optional[NonEmptyStr]

    earning_to_author: PositiveFloat
    earning_to_voter: PositiveFloat

    @classmethod
    async def _create_table(cls) -> None:
        await jianshu_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS article_earning_ranking_records (
                date DATE NOT NULL,
                ranking SMALLINT NOT NULL,
                article_slug VARCHAR(12),
                article_title TEXT,
                author_slug VARCHAR(12),
                earning_to_author NUMERIC NOT NULL,
                earning_to_voter NUMERIC NOT NULL,
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
            "article_slug, article_title, author_slug, earning_to_author, "
            "earning_to_voter) VALUES (%s, %s, %s, %s, %s, %s, %s);",
            [
                (
                    item.date,
                    item.ranking,
                    item.article_slug,
                    item.article_title,
                    item.author_slug,
                    item.earning_to_author,
                    item.earning_to_voter,
                )
                for item in data
            ],
        )
