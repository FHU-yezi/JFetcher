from __future__ import annotations

from datetime import datetime
from typing import Literal

from jkit.user import MembershipType
from sshared.postgres import Table
from sshared.strict_struct import NonEmptyStr, PositiveInt

from utils.db import jianshu_pool

StatusType = Literal["NORMAL", "INACCESSIBLE"]

# HACK: 使用该常量标识会员信息不可用，跳过更新
MEMBERSHIP_INFO_UNAVALIABLE = "MEMBERSHIP_INFO_UNAVALIABLE"

class User(Table, frozen=True):
    slug: NonEmptyStr
    status: StatusType
    update_time: datetime
    id: PositiveInt
    name: NonEmptyStr
    history_names: list[NonEmptyStr]
    avatar_url: NonEmptyStr | None
    membership_type: MembershipType
    membership_expire_time: datetime | None

    @classmethod
    async def create(  # noqa: PLR0913
        cls,
        *,
        slug: str,
        id: int,
        name: str,
        avatar_url: str | None,
        membership_type: MembershipType,
        membership_expire_time: datetime | None,
    ) -> None:
        async with jianshu_pool.get_conn() as conn:
            await conn.execute(
                "INSERT INTO users (slug, status, update_time, id, name, "
                "history_names, avatar_url, membership_type, membership_expire_time) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);",
                (
                    slug,
                    "NORMAL",
                    datetime.now(),
                    id,
                    name,
                    [],
                    avatar_url,
                    membership_type,
                    membership_expire_time,
                ),
            )

    @classmethod
    async def get_by_slug(cls, slug: str, /) -> User | None:
        async with jianshu_pool.get_conn() as conn:
            cursor = await conn.execute(
                "SELECT status, update_time, id, name, history_names, "
                "avatar_url, membership_type, membership_expxire_time "
                "FROM users WHERE slug = %s;",
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
            membership_type=data[6],
            membership_expire_time=data[7],
        ).validate()

    @classmethod
    async def update_by_slug(
        cls,
        *,
        slug: str,
        name: str,
        avatar_url: str,
        # HACK
        membership_type: MembershipType | str,
        membership_expire_time: datetime | None | str,
    ) -> None:
        old_data = await cls.get_by_slug(slug)
        if old_data is None:
            raise ValueError

        # 避免竞争更新导致数据过时
        if old_data.update_time > datetime.now():
            return

        async with jianshu_pool.get_conn() as conn, conn.transaction():
            # 更新 update_time
            await conn.execute(
                "UPDATE users SET update_time = %s WHERE slug = %s;",
                (datetime.now(), slug),
            )

            # 如果 name 已修改
            if name != old_data.name:
                # 更新 name
                await conn.execute(
                    "UPDATE users SET name = %s WHERE slug = %s;",
                    (name, slug),
                )

                # 将旧数据的 name 添加到 history_names 中
                await conn.execute(
                    "UPDATE users SET history_names = "
                    "array_append(history_names, %s) WHERE slug = %s;",
                    (old_data.name, slug),
                )

            # 如果旧数据中不存在 avatar_url 或 avatar_url 已修改
            if old_data.avatar_url is None or avatar_url != old_data.avatar_url:
                # 更新 avatar_url
                await conn.execute(
                    "UPDATE users SET avatar_url = %s WHERE slug = %s;",
                    (avatar_url, slug),
                )

            # 如果 membership_type 已修改
            if membership_type != old_data.membership_type:
                # 更新 membership_type 和 membership_expire_time
                await conn.execute(
                    "UPDATE users SET membership_type = %s, "
                    "membership_expire_time = %s WHERE slug = %s;",
                    (membership_type, membership_expire_time, slug),
                )
            # 如果 membership_expire_time 已修改
            elif membership_expire_time != old_data.membership_expire_time:
                # 更新 membership_expire_time
                await conn.execute(
                    "UPDATE users SET membership_expire_time = %s WHERE slug = %s;",
                    (membership_expire_time, slug),
                )
