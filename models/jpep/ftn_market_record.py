from __future__ import annotations

from datetime import datetime

from sshared.postgres import Table
from sshared.strict_struct import NonNegativeInt, PositiveFloat, PositiveInt

from utils.db import jpep_pool


class FtnMarketRecord(Table, frozen=True):
    fetch_time: datetime
    id: PositiveInt
    price: PositiveFloat
    total_amount: PositiveInt
    traded_amount: NonNegativeInt
    remaining_amount: int
    minimum_trade_amount: PositiveInt
    completed_trades_count: NonNegativeInt

    @classmethod
    async def create(  # noqa: PLR0913
        cls,
        *,
        fetch_time: datetime,
        id: int,
        price: float,
        total_amount: int,
        traded_amount: int,
        remaining_amount: int,
        minimum_trade_amount: int,
        completed_trades_count: int,
    ) -> None:
        async with jpep_pool.get_conn() as conn:
            await conn.cursor().execute(
                "INSERT INTO ftn_market_records (fetch_time, id, price, total_amount, "
                "traded_amount, remaining_amount, minimum_trade_amount, "
                "completed_trades_count) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s);",
                (
                    fetch_time,
                    id,
                    price,
                    total_amount,
                    traded_amount,
                    remaining_amount,
                    minimum_trade_amount,
                    completed_trades_count,
                ),
            )

    @classmethod
    async def exists_by_fetch_time(cls, fetch_time: datetime, /) -> bool:
        async with jpep_pool.get_conn() as conn:
            cursor = await conn.execute(
                "SELECT 1 FROM ftn_market_records WHERE fetch_time = %s LIMIT 1;",
                (fetch_time,),
            )

            return await cursor.fetchone() is not None
