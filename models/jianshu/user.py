from __future__ import annotations

from datetime import datetime
from enum import Enum

from sshared.postgres import Table
from sshared.strict_struct import NonEmptyStr, PositiveInt

from utils.db import jianshu_pool


class StatusEnum(Enum):
    NORMAL = "NORMAL"
    INACCESSIBLE = "INACCESSIBLE"


class User(Table, frozen=True):
    slug: NonEmptyStr
    status: StatusEnum
    update_time: datetime
    id: PositiveInt
    name: NonEmptyStr
    history_names: list[NonEmptyStr]
    avatar_url: NonEmptyStr

    async def create(self) -> None:
        self.validate()

        async with jianshu_pool.get_conn() as conn:
            await conn.execute(
                "INSERT INTO users (slug, status, update_time, id, name, "
                "history_names, avatar_url) VALUES (%s, %s, %s, %s, %s, %s, %s);",
                (
                    self.slug,
                    self.status,
                    self.update_time,
                    self.id,
                    self.name,
                    self.history_names,
                    self.avatar_url,
                ),
            )

    @classmethod
    async def get_by_slug(cls, slug: str) -> User | None:
        async with jianshu_pool.get_conn() as conn:
            cursor = await conn.execute(
                "SELECT status, update_time, id, name, history_names, "
                "avatar_url FROM users WHERE slug = %s;",
                (slug,),
            )

            data = await cursor.fetchone()
        if not data:
            return None

        return cls(
            slug=slug,
            status=data[0],
            update_time=data[1],
            id=data[2],
            name=data[3],
            history_names=data[4],
            avatar_url=data[5],
        )

    @classmethod
    async def upsert(
        cls,
        slug: str,
        id: int,
        name: str,
        avatar_url: str,
    ) -> None:
        user = await cls.get_by_slug(slug)
        # 如果不存在，创建用户
        if not user:
            await cls(
                slug=slug,
                status=StatusEnum.NORMAL,
                update_time=datetime.now(),
                id=id,
                name=name,
                history_names=[],
                avatar_url=avatar_url,
            ).create()
            return

        # 如果当前数据不是最新，跳过更新
        if user.update_time > datetime.now():
            return

        # 在一个事务中一次性完成全部字段的更新
        async with jianshu_pool.get_conn() as conn, conn.transaction():
            # 更新更新时间
            await conn.execute(
                "UPDATE users SET update_time = %s WHERE slug = %s;",
                (datetime.now(), slug),
            )

            # ID 无法被修改，如果异常则抛出错误
            if user.id and id and user.id != id:
                raise ValueError(f"用户 ID 不一致：{user.id} != {id}")

            # 如果没有存储 ID，进行添加
            if not user.id and id:
                await conn.execute(
                    "UPDATE users SET id = %s WHERE slug = %s;",
                    (id, slug),
                )

            # 如果没有存储昵称，进行添加
            if not user.name and name:
                await conn.execute(
                    "UPDATE users SET name = %s WHERE slug = %s;",
                    (name, slug),
                )

            # 更新昵称
            if user.name and name and user.name != name:
                await conn.execute(
                    "UPDATE users SET name = %s WHERE slug = %s;",
                    (name, slug),
                )
                await conn.execute(
                    "UPDATE users SET history_names = array_append(history_names, "
                    "%s) WHERE slug = %s;",
                    (user.name, slug),
                )

            # 如果没有存储头像链接，进行添加
            if not user.avatar_url and avatar_url:
                await conn.execute(
                    "UPDATE users SET avatar_url = %s WHERE slug = %s;",
                    (avatar_url, slug),
                )

            # 更新头像链接
            if user.avatar_url and avatar_url and user.avatar_url != avatar_url:
                await conn.execute(
                    "UPDATE users SET avatar_url = %s WHERE slug = %s;",
                    (avatar_url, slug),
                )
