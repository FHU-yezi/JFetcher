from datetime import datetime

from sshared.postgres import Table
from sshared.strict_struct import NonNegativeInt, PositiveFloat, PositiveInt

from utils.db import jpep_pool


class FTNMacketRecord(Table, frozen=True):
    fetch_time: datetime
    id: PositiveInt
    price: PositiveFloat
    traded_count: NonNegativeInt
    total_amount: PositiveInt
    traded_amount: NonNegativeInt
    remaining_amount: int
    minimum_trade_amount: PositiveInt

    @classmethod
    async def insert_many(cls, data: list["FTNMacketRecord"]) -> None:
        for item in data:
            item.validate()

        async with jpep_pool.get_conn() as conn:
            await conn.cursor().executemany(
                "INSERT INTO ftn_macket_records (fetch_time, id, price, traded_count, "
                "total_amount, traded_amount, remaining_amount, minimum_trade_amount) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s);",
                [
                    (
                        item.fetch_time,
                        item.id,
                        item.price,
                        item.traded_count,
                        item.total_amount,
                        item.traded_amount,
                        item.remaining_amount,
                        item.minimum_trade_amount,
                    )
                    for item in data
                ],
            )
