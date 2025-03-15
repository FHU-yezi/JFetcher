from __future__ import annotations

from datetime import datetime
from typing import Literal

from sshared.postgres import Table
from sshared.strict_struct import NonNegativeInt, PositiveFloat, PositiveInt

from utils.db import jpep_pool

FtnMarketSummaryRecordType = Literal["BUY", "SELL"]


class FtnMarketSummaryRecord(Table, frozen=True):
    fetch_time: datetime
    type: FtnMarketSummaryRecordType
    best_price: PositiveFloat
    total_amount: PositiveInt
    traded_amount: NonNegativeInt
    remaining_amount: int

    @classmethod
    async def create(  # noqa: PLR0913
        cls,
        *,
        fetch_time: datetime,
        type: FtnMarketSummaryRecordType,
        best_price: float,
        total_amount: int,
        traded_amount: int,
        remaining_amount: int,
    ) -> None:
        async with jpep_pool.get_conn() as conn:
            await conn.cursor().execute(
                "INSERT INTO ftn_market_summary_records (fetch_time, type, best_price, "
                "total_amount, traded_amount, remaining_amount) "
                "VALUES (%s, %s, %s, %s, %s, %s);",
                (
                    fetch_time,
                    type,
                    best_price,
                    total_amount,
                    traded_amount,
                    remaining_amount,
                ),
            )

    @classmethod
    async def create_from_ftn_market_records_by_fetch_time_and_type(
        cls, *, fetch_time: datetime, type: FtnMarketSummaryRecordType
    ) -> None:
        async with jpep_pool.get_conn() as conn:
            if type == "BUY":
                cursor = await conn.cursor().execute(
                    "SELECT MIN(price), SUM(total_amount), SUM(traded_amount), "
                    "SUM(remaining_amount) FROM ftn_market_records "
                    "JOIN ftn_orders ON ftn_market_records.id = ftn_orders.id "
                    "WHERE type = 'BUY' AND fetch_time = %s;",
                    (fetch_time,),
                )
            else:
                cursor = await conn.cursor().execute(
                    "SELECT MAX(price), SUM(total_amount), SUM(traded_amount), "
                    "SUM(remaining_amount) FROM ftn_market_records "
                    "JOIN ftn_orders ON ftn_market_records.id = ftn_orders.id "
                    "WHERE type = 'SELL' AND fetch_time = %s;",
                    (fetch_time,),
                )

            data = await cursor.fetchone()

        # 如果没有订单数据，不做任何操作
        if not data or data[0] is None:
            return

        best_price, total_amount, traded_amount, remaining_amount = data
        await cls.create(
            fetch_time=fetch_time,
            type=type,
            best_price=best_price,
            total_amount=total_amount,
            traded_amount=traded_amount,
            remaining_amount=remaining_amount,
        )
