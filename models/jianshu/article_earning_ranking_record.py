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
    async def create(  # noqa: PLR0913
        cls,
        *,
        date: date,
        ranking: int,
        slug: str | None,
        title: str | None,
        author_slug: str | None,
        author_earning: float,
        voter_earning: float,
    ) -> None:
        async with jianshu_pool.get_conn() as conn:
            await conn.cursor().execute(
                "INSERT INTO article_earning_ranking_records (date, ranking, "
                "slug, title, author_slug, author_earning, voter_earning) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s);",
                (
                    date,
                    ranking,
                    slug,
                    title,
                    author_slug,
                    author_earning,
                    voter_earning,
                ),
            )

    @classmethod
    async def count_by_date(cls, date: date, /) -> int:
        async with jianshu_pool.get_conn() as conn:
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM article_earning_ranking_records WHERE date = %s;",
                (date,),
            )

            data = await cursor.fetchone()
            if not data:
                raise ValueError

        return data[0]
