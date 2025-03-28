from __future__ import annotations

from datetime import datetime

from sshared.postgres import Table
from sshared.strict_struct import NonNegativeInt, PositiveFloat, PositiveInt

from utils.db import beijiaoyi_pool


class FtnMarketRecord(Table, frozen=True):
    fetch_time: datetime
    id: PositiveInt
    price: PositiveFloat
    total_amount: PositiveInt
    traded_amount: NonNegativeInt
    remaining_amount: int
    minimum_trade_amount: PositiveInt
    maximum_trade_amount: PositiveInt | None
    completed_trades_count: NonNegativeInt

    @classmethod
    async def create(
        cls,
        *,
        fetch_time: datetime,
        id: int,
        price: float,
        total_amount: int,
        traded_amount: int,
        remaining_amount: int,
        minimum_trade_amount: int,
        maximum_trade_amount: int | None,
        completed_trades_count: int,
    ) -> None:
        async with beijiaoyi_pool.get_conn() as conn:
            await conn.cursor().execute(
                "INSERT INTO ftn_market_records (fetch_time, id, price, total_amount, "
                "traded_amount, remaining_amount, minimum_trade_amount, "
                "maximum_trade_amount, completed_trades_count) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);",
                (
                    fetch_time,
                    id,
                    price,
                    total_amount,
                    traded_amount,
                    remaining_amount,
                    minimum_trade_amount,
                    maximum_trade_amount,
                    completed_trades_count,
                ),
            )

    @classmethod
    async def exists_by_fetch_time(cls, fetch_time: datetime, /) -> bool:
        async with beijiaoyi_pool.get_conn() as conn:
            cursor = await conn.execute(
                "SELECT 1 FROM ftn_market_records WHERE fetch_time = %s LIMIT 1;",
                (fetch_time,),
            )

            return await cursor.fetchone() is not None
