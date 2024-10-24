from datetime import datetime
from enum import Enum
from typing import Optional

from sshared.postgres import Table, create_enum
from sshared.strict_struct import NonEmptyStr, PositiveInt

from utils.postgres import jianshu_conn


class StatusEnum(Enum):
    NORMAL = "NORMAL"
    INACCESSIBLE = "INACCESSIBLE"


class User(Table, frozen=True):
    slug: NonEmptyStr
    status: StatusEnum
    id: Optional[PositiveInt]
    name: Optional[NonEmptyStr]
    update_time: datetime
    history_names: list[NonEmptyStr]
    avatar_url: Optional[NonEmptyStr]

    @classmethod
    async def _create_enum(cls) -> None:
        await create_enum(
            conn=jianshu_conn, name="enum_users_status", enum_class=StatusEnum
        )

    @classmethod
    async def _create_table(cls) -> None:
        await jianshu_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                slug VARCHAR(12) CONSTRAINT pk_users_slug PRIMARY KEY,
                status enum_users_status NOT NULL,
                id INTEGER,
                name VARCHAR(15),
                update_time TIMESTAMP NOT NULL,
                history_names VARCHAR(15)[] NOT NULL,
                avatar_url TEXT
            );
            """
        )

    async def create(self) -> None:
        self.validate()
        await jianshu_conn.execute(
            "INSERT INTO users (slug, status, id, name, update_time, "
            "history_names, avatar_url) VALUES (%s, %s, %s, %s, %s, %s, %s);",
            (
                self.slug,
                self.status,
                self.id,
                self.name,
                self.update_time,
                self.history_names,
                self.avatar_url,
            ),
        )
