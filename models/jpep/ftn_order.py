from __future__ import annotations

from datetime import datetime
from enum import Enum

from sshared.postgres import Table
from sshared.strict_struct import PositiveInt

from utils.db import jpep_pool


class TypeEnum(Enum):
    BUY = "BUY"
    SELL = "SELL"


class FTNOrder(Table, frozen=True):
    id: PositiveInt
    type: TypeEnum
    publisher_id: PositiveInt
    publish_time: datetime
    last_seen_time: datetime | None

    async def create(self) -> None:
        async with jpep_pool.get_conn() as conn:
            await conn.execute(
                "INSERT INTO ftn_orders (id, type, publisher_id, publish_time, "
                "last_seen_time) VALUES (%s, %s, %s, %s, %s);",
                (
                    self.id,
                    self.type,
                    self.publisher_id,
                    self.publish_time,
                    self.last_seen_time,
                ),
            )

    @classmethod
    async def get_by_id(cls, id: int) -> FTNOrder | None:  # noqa: A002
        async with jpep_pool.get_conn() as conn:
            cursor = await conn.execute(
                "SELECT type, publisher_id, publish_time, last_seen_time "
                "FROM ftn_orders WHERE id = %s;",
                (id,),
            )

            data = await cursor.fetchone()
        if not data:
            return None

        return cls(
            id=id,
            type=data[0],
            publisher_id=data[1],
            publish_time=data[2],
            last_seen_time=data[3],
        )

    @classmethod
    async def update_last_seen_time(cls, id: int, last_seen_time: datetime) -> None:  # noqa: A002
        async with jpep_pool.get_conn() as conn:
            await conn.execute(
                "UPDATE ftn_orders SET last_seen_time = %s WHERE id = %s;",
                (last_seen_time, id),
            )
