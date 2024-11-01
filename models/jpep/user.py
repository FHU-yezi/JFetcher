from datetime import datetime
from typing import Optional

from sshared.postgres import Table
from sshared.strict_struct import (
    NonEmptyStr,
    PositiveInt,
)

from utils.postgres import get_jpep_conn


class User(Table, frozen=True):
    id: PositiveInt
    update_time: datetime
    name: NonEmptyStr
    hashed_name: NonEmptyStr
    avatar_url: Optional[NonEmptyStr]

    @classmethod
    async def _create_table(cls) -> None:
        conn = await get_jpep_conn()
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER NOT NULL CONSTRAINT pk_users_id PRIMARY KEY,
                update_time TIMESTAMP NOT NULL,
                name TEXT NOT NULL,
                hashed_name VARCHAR(9) NOT NULL,
                avatar_url TEXT
            );
            """
        )

    async def create(self) -> None:
        self.validate()

        conn = await get_jpep_conn()
        await conn.execute(
            "INSERT INTO users (id, update_time, name, hashed_name, avatar_url) VALUES "
            "(%s, %s, %s, %s, %s);",
            (self.id, self.update_time, self.name, self.hashed_name, self.avatar_url),
        )

    @classmethod
    async def get_by_id(cls, id: int) -> Optional["User"]:  # noqa: A002
        conn = await get_jpep_conn()
        cursor = await conn.execute(
            "SELECT update_time, name, hashed_name, avatar_url "
            "FROM users WHERE id = %s",
            (id,),
        )

        data = await cursor.fetchone()
        if not data:
            return None

        return cls(
            id=id,
            update_time=data[0],
            name=data[1],
            hashed_name=data[2],
            avatar_url=data[3],
        )

    @classmethod
    async def upsert(
        cls,
        id: int,  # noqa: A002
        name: str,
        hashed_name: str,
        avatar_url: Optional[str] = None,
    ) -> None:
        user = await cls.get_by_id(id)
        # 如果不存在，创建用户
        if not user:
            await cls(
                id=id,
                update_time=datetime.now(),
                name=name,
                hashed_name=hashed_name,
                avatar_url=avatar_url,
            ).create()
            return

        # 如果当前数据不是最新，跳过更新
        if user.update_time > datetime.now():
            return

        # 在一个事务中一次性完成全部字段的更新
        conn = await get_jpep_conn()
        async with conn.transaction():
            # 更新更新时间
            await conn.execute(
                "UPDATE users SET update_time = %s WHERE id = %s",
                (datetime.now(), id),
            )

            # 更新昵称和哈希后昵称
            if user.name and name and user.name != name:
                # 哈希后昵称一定会跟随昵称变化，一同更新
                await conn.execute(
                    "UPDATE users SET name = %s, hashed_name = %s WHERE id = %s",
                    (name, hashed_name, id),
                )

            # 如果没有存储头像链接，进行添加
            if not user.avatar_url and avatar_url:
                await conn.execute(
                    "UPDATE users SET avatar_url = %s WHERE id = %s",
                    (avatar_url, id),
                )

            # 更新头像链接
            if user.avatar_url and avatar_url and user.avatar_url != avatar_url:
                await conn.execute(
                    "UPDATE users SET avatar_url = %s WHERE id = %s",
                    (avatar_url, id),
                )
