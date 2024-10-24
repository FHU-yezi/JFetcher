from asyncio import run as asyncio_run

from sshared.logging import Logger

from models.jianshu.daily_update_ranking_record import DailyUpdateRankingRecordDocument
from models.new.jianshu.daily_update_ranking_record import DailyUpdateRankingRecord

logger = Logger()


async def main() -> None:
    await DailyUpdateRankingRecord.init()
    logger.debug("初始化数据库表完成")

    batch: list[DailyUpdateRankingRecord] = []

    async for item in DailyUpdateRankingRecordDocument.Meta.collection.find().sort(
        {"date": 1, "ranking": 1}
    ):
        batch.append(
            DailyUpdateRankingRecord(
                date=item["date"].date(),
                ranking=item["ranking"],
                slug=item["userSlug"],
                days=item["days"],
            )
        )
        if len(batch) == 5000:
            await DailyUpdateRankingRecord.insert_many(batch)
            logger.debug(
                f"迁移 {batch[0].date}/{batch[0].ranking} - "
                f"{batch[-1].date}/{batch[-1].ranking} 完成"
            )
            batch.clear()

    if batch:
        await DailyUpdateRankingRecord.insert_many(batch)
        logger.debug(
            f"迁移 {batch[0].date}/{batch[0].ranking} - "
            f"{batch[-1].date}/{batch[-1].ranking} 完成"
        )
        batch.clear()

    logger.info("数据迁移完成")


asyncio_run(main())
