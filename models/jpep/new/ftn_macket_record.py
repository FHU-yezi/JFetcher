from datetime import datetime

from sshared.postgres import Table
from sshared.strict_struct import NonNegativeInt, PositiveFloat, PositiveInt

from utils.postgres import get_jpep_conn


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
    async def _create_table(cls) -> None:
        conn = await get_jpep_conn()
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ftn_macket_records (
                fetch_time TIMESTAMP NOT NULL,
                id INTEGER NOT NULL,
                price NUMERIC NOT NULL,
                traded_count SMALLINT NOT NULL,
                total_amount INTEGER NOT NULL,
                traded_amount INTEGER NOT NULL,
                remaining_amount INTEGER NOT NULL,
                minimum_trade_amount INTEGER NOT NULL,
                CONSTRAINT pk_ftn_macket_records_fetch_time_id PRIMARY KEY (fetch_time, id)
            );
            """  # noqa: E501
        )

    @classmethod
    async def insert_many(cls, data: list["FTNMacketRecord"]) -> None:
        for item in data:
            item.validate()

        conn = await get_jpep_conn()
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
