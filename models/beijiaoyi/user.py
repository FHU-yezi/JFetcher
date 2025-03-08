from __future__ import annotations

from datetime import datetime

from sshared.postgres import Table
from sshared.strict_struct import (
    NonEmptyStr,
    PositiveInt,
)

from utils.db import beijiaoyi_pool


class User(Table, frozen=True):
    id: PositiveInt
    update_time: datetime
    name: NonEmptyStr
    avatar_url: NonEmptyStr

    @classmethod
    async def create(cls, *, id: int, name: str, avatar_url: str) -> None:
        async with beijiaoyi_pool.get_conn() as conn:
            await conn.execute(
                "INSERT INTO users (id, update_time, name, avatar_url) "
                "VALUES (%s, %s, %s, %s);",
                (
                    id,
                    datetime.now(),
                    name,
                    avatar_url,
                ),
            )

    @classmethod
    async def get_by_id(cls, id: int, /) -> User | None:
        async with beijiaoyi_pool.get_conn() as conn:
            cursor = await conn.execute(
                "SELECT update_time, name, avatar_url FROM users WHERE id = %s;",
                (id,),
            )

            data = await cursor.fetchone()
        if not data:
            return None

        return cls(
            id=id,
            update_time=data[0],
            name=data[1],
            avatar_url=data[2],
        ).validate()

    @classmethod
    async def update_by_id(cls, *, id: int, name: str, avatar_url: str) -> None:
        old_data = await cls.get_by_id(id)
        if old_data is None:
            raise ValueError

        # 避免竞争更新导致数据过时
        if old_data.update_time > datetime.now():
            return

        async with beijiaoyi_pool.get_conn() as conn, conn.transaction():
            # 更新 update_time
            await conn.execute(
                "UPDATE users SET update_time = %s WHERE id = %s;",
                (datetime.now(), id),
            )

            # 如果 name 已修改
            if name != old_data.name:
                # 更新 name
                await conn.execute(
                    "UPDATE users SET name = %s WHERE id = %s;",
                    (name, id),
                )

            # 如果 avatar_url 已修改
            if avatar_url != old_data.avatar_url:
                # 更新 avatar_url
                await conn.execute(
                    "UPDATE users SET avatar_url = %s WHERE id = %s;",
                    (avatar_url, id),
                )
