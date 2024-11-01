from asyncio import run as asyncio_run

from sshared.logging import Logger

from models.jpep.new.ftn_macket_record import FTNMacketRecord
from utils.mongo import JPEP_DB

OLD_COLLECTION = JPEP_DB.ftn_trade_orders
logger = Logger()


async def main() -> None:
    await FTNMacketRecord.init()

    batch: list[FTNMacketRecord] = []

    logger.info("开始执行数据迁移")
    async for item in OLD_COLLECTION.find().sort({"fetchTime": 1}):
        batch.append(
            FTNMacketRecord(
                fetch_time=item["fetchTime"],
                id=item["id"],
                price=item["price"],
                traded_count=item["tradedCount"],
                total_amount=item["amount"]["total"],
                traded_amount=item["amount"]["traded"],
                remaining_amount=item["amount"]["tradable"],
                minimum_trade_amount=item["amount"]["minimumTrade"],
            )
        )

        if len(batch) == 10000:
            await FTNMacketRecord.insert_many(batch)
            logger.debug(
                f"已迁移 {batch[0].fetch_time}/{batch[0].id} - "
                f"{batch[-1].fetch_time}/{batch[-1].id}"
            )
            batch.clear()

    if batch:
        await FTNMacketRecord.insert_many(batch)
        logger.debug(
            f"已迁移 {batch[0].fetch_time}/{batch[0].id} - "
            f"{batch[-1].fetch_time}/{batch[-1].id}"
        )
        batch.clear()

    logger.info("数据迁移完成")


asyncio_run(main())
