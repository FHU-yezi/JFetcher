from __future__ import annotations

from datetime import datetime

from sshared.postgres import Table
from sshared.strict_struct import NonEmptyStr, NonNegativeFloat

from utils.db import jianshu_pool


class CoreUserAssetsRecord(Table, frozen=True):
    time: datetime
    slug: NonEmptyStr

    fp: NonNegativeFloat | None
    ftn: NonNegativeFloat | None
    assets: NonNegativeFloat | None

    @classmethod
    async def create(
        cls,
        *,
        time: datetime,
        slug: str,
        fp: float | None,
        ftn: float | None,
        assets: float | None,
    ) -> None:
        async with jianshu_pool.get_conn() as conn:
            await conn.cursor().execute(
                "INSERT INTO core_user_assets_records (time, slug, "
                "fp, ftn, assets) VALUES (%s, %s, %s, %s, %s);",
                (
                    time,
                    slug,
                    fp,
                    ftn,
                    assets,
                ),
            )

    @classmethod
    async def exists_by_time_and_slug(cls, *, time: datetime, slug: str) -> int:
        async with jianshu_pool.get_conn() as conn:
            cursor = await conn.execute(
                "SELECT 1 FROM core_user_assets_records "
                "WHERE time = %s AND slug = %s LIMIT 1;",
                (time, slug),
            )

            return await cursor.fetchone() is not None
