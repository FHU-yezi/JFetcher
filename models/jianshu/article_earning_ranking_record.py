from __future__ import annotations

from datetime import date

from sshared.postgres import Table
from sshared.strict_struct import NonEmptyStr, PositiveFloat, PositiveInt

from utils.db import jianshu_pool


class ArticleEarningRankingRecord(Table, frozen=True):
    date: date
    ranking: PositiveInt
    slug: NonEmptyStr | None
    title: NonEmptyStr | None
    author_slug: NonEmptyStr | None

    author_earning: PositiveFloat
    voter_earning: PositiveFloat

    @classmethod
    async def insert_many(cls, data: list[ArticleEarningRankingRecord]) -> None:
        for item in data:
            item.validate()

        async with jianshu_pool.get_conn() as conn:
            await conn.cursor().executemany(
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
