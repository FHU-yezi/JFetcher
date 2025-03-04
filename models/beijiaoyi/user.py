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

    async def create(self) -> None:
        self.validate()

        async with beijiaoyi_pool.get_conn() as conn:
            await conn.execute(
                "INSERT INTO users (id, update_time, name, avatar_url) "
                "VALUES (%s, %s, %s, %s);",
                (
                    self.id,
                    self.update_time,
                    self.name,
                    self.avatar_url,
                ),
            )

    @classmethod
    async def get_by_id(cls, id: int) -> User | None:
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
        )

    @classmethod
    async def upsert(
        cls,
        id: int,
        name: str,
        avatar_url: str,
    ) -> None:
        user = await cls.get_by_id(id)
        # 如果不存在，创建用户
        if not user:
            await cls(
                id=id,
                update_time=datetime.now(),
                name=name,
                avatar_url=avatar_url,
            ).create()
            return

        # 如果当前数据不是最新，跳过更新
        if user.update_time > datetime.now():
            return

        # 在一个事务中一次性完成全部字段的更新
        async with beijiaoyi_pool.get_conn() as conn, conn.transaction():
            # 更新更新时间
            await conn.execute(
                "UPDATE users SET update_time = %s WHERE id = %s;",
                (datetime.now(), id),
            )

            # 更新昵称
            if user.name != name:
                # 哈希后昵称一定会跟随昵称变化，一同更新
                await conn.execute(
                    "UPDATE users SET name = %s WHERE id = %s;",
                    (name, id),
                )

            # 更新头像链接
            if user.avatar_url != avatar_url:
                await conn.execute(
                    "UPDATE users SET avatar_url = %s WHERE id = %s;",
                    (avatar_url, id),
                )
