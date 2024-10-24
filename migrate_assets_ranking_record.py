from asyncio import run as asyncio_run

from sshared.logging import Logger

from models.jianshu.assets_ranking_record import (
    AssetsRankingRecordDocument,
)
from models.new.jianshu.assets_ranking_record import (
    AssetsRankingRecord,
)

logger = Logger()


async def main() -> None:
    await AssetsRankingRecord.init()
    logger.debug("初始化数据库表完成")

    batch: list[AssetsRankingRecord] = []

    async for item in AssetsRankingRecordDocument.Meta.collection.find().sort(
        {"date": 1, "ranking": 1}
    ):
        batch.append(
            AssetsRankingRecord(
                date=item["date"].date(),
                ranking=item["ranking"],
                user_slug=item["userSlug"],
                fp=item["amount"]["fp"],
                ftn=item["amount"]["ftn"],
                assets=item["amount"]["assets"],
            )
        )
        if len(batch) == 5000:
            await AssetsRankingRecord.insert_many(batch)
            logger.debug(
                f"迁移 {batch[0].date}/{batch[0].ranking} - "
                f"{batch[-1].date}/{batch[-1].ranking} 完成"
            )
            batch.clear()

    if batch:
        await AssetsRankingRecord.insert_many(batch)
        logger.debug(
            f"迁移 {batch[0].date}/{batch[0].ranking} - "
            f"{batch[-1].date}/{batch[-1].ranking} 完成"
        )
        batch.clear()

    logger.info("数据迁移完成")


asyncio_run(main())
