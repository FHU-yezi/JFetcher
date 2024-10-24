from asyncio import run as asyncio_run

from sshared.logging import Logger

from models.jianshu.lottery_win_record import (
    LotteryWinRecordDocument,
)
from models.new.jianshu.lottery_win_record import (
    LotteryWinRecord,
)

logger = Logger()


async def main() -> None:
    await LotteryWinRecord.init()
    logger.debug("初始化数据库表完成")

    batch: list[LotteryWinRecord] = []

    async for item in LotteryWinRecordDocument.Meta.collection.find().sort({"id": 1}):
        batch.append(
            LotteryWinRecord(
                id=item["id"],
                time=item["time"],
                user_slug=item["userSlug"],
                award_name=item["awardName"],
            )
        )
        if len(batch) == 5000:
            await LotteryWinRecord.insert_many(batch)
            logger.debug(f"迁移 {batch[0].id} - {batch[-1].id} 完成")
            batch.clear()

    if batch:
        await LotteryWinRecord.insert_many(batch)
        logger.debug(f"迁移 {batch[0].id} - {batch[-1].id} 完成")
        batch.clear()

    logger.info("数据迁移完成")


asyncio_run(main())
