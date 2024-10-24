from asyncio import run as asyncio_run

from sshared.logging import Logger

from models.jianshu.assets_ranking_record import (
    AssetsRankingRecordDocument,
)
from models.new.jianshu.user_assets_ranking_record import (
    UserAssetsRankingRecord,
)

logger = Logger()


async def main() -> None:
    await UserAssetsRankingRecord.init()
    logger.debug("初始化数据库表完成")

    batch: list[UserAssetsRankingRecord] = []

    async for item in AssetsRankingRecordDocument.Meta.collection.find().sort(
        {"date": 1, "ranking": 1}
    ):
        batch.append(
            UserAssetsRankingRecord(
                date=item["date"].date(),
                ranking=item["ranking"],
                slug=item["userSlug"],
                fp=item["amount"]["fp"],
                ftn=item["amount"]["ftn"],
                assets=item["amount"]["assets"],
            )
        )
        if len(batch) == 5000:
            await UserAssetsRankingRecord.insert_many(batch)
            logger.debug(
                f"迁移 {batch[0].date}/{batch[0].ranking} - "
                f"{batch[-1].date}/{batch[-1].ranking} 完成"
            )
            batch.clear()

    if batch:
        await UserAssetsRankingRecord.insert_many(batch)
        logger.debug(
            f"迁移 {batch[0].date}/{batch[0].ranking} - "
            f"{batch[-1].date}/{batch[-1].ranking} 完成"
        )
        batch.clear()

    logger.info("数据迁移完成")


asyncio_run(main())
