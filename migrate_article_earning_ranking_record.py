from asyncio import run as asyncio_run

from sshared.logging import Logger

from models.jianshu.article_earning_ranking_record import (
    ArticleEarningRankingRecordDocument,
)
from models.new.jianshu.article_earning_ranking_record import (
    ArticleEarningRankingRecord,
)

logger = Logger()


async def main() -> None:
    await ArticleEarningRankingRecord.init()
    logger.debug("初始化数据库表完成")

    batch: list[ArticleEarningRankingRecord] = []

    async for item in ArticleEarningRankingRecordDocument.Meta.collection.find().sort(
        {"date": 1, "ranking": 1}
    ):
        batch.append(
            ArticleEarningRankingRecord(
                date=item["date"].date(),
                ranking=item["ranking"],
                article_slug=item["article"]["slug"],
                article_title=item["article"]["title"],
                author_slug=item["authorSlug"],
                earning_to_author=item["earning"]["toAuthor"],
                earning_to_voter=item["earning"]["toVoter"],
            )
        )
        if len(batch) == 5000:
            await ArticleEarningRankingRecord.insert_many(batch)
            logger.debug(
                f"迁移 {batch[0].date}/{batch[0].ranking} - "
                f"{batch[-1].date}/{batch[-1].ranking} 完成"
            )
            batch.clear()

    if batch:
        await ArticleEarningRankingRecord.insert_many(batch)
        logger.debug(
            f"迁移 {batch[0].date}/{batch[0].ranking} - "
            f"{batch[-1].date}/{batch[-1].ranking} 完成"
        )
        batch.clear()

    logger.info("数据迁移完成")


asyncio_run(main())
