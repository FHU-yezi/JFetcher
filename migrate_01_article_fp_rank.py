import asyncio
from datetime import datetime
from typing import List

from jkit.identifier_convert import article_url_to_slug, user_url_to_slug
from sspeedup.logging.run_logger import RunLogger

from models.jianshu.article_earning_ranking_record import (
    ArticleEarningRankingRecordDocument,
    ArticleField,
    EarningField,
)
from models.jianshu.user import JianshuUserDocument
from old_models.article_fp_rank import OldArticleFPRank
from utils.migrate_helper import (
    get_collection_data_count,
    get_collection_data_time_range,
)

logger = RunLogger()


async def ensure_all_old_collection_indexes() -> None:
    await OldArticleFPRank.ensure_indexes()


async def ensure_all_new_collection_indexes() -> None:
    await JianshuUserDocument.ensure_indexes()
    await ArticleEarningRankingRecordDocument.ensure_indexes()


async def insert_or_update_user(item: OldArticleFPRank) -> None:
    if item.author.url:
        await JianshuUserDocument.insert_or_update_one(
            slug=user_url_to_slug(item.author.url),
            updated_at=datetime.fromisoformat(item.date.isoformat()),
            id=item.author.id,
            name=item.author.name,
        )


async def convert_item(item: OldArticleFPRank) -> ArticleEarningRankingRecordDocument:
    return ArticleEarningRankingRecordDocument(
        date=item.date,
        ranking=item.ranking,
        article=ArticleField(
            slug=article_url_to_slug(item.article.url) if item.article.url else None,
            title=item.article.title,
        ),
        author_slug=user_url_to_slug(item.author.url) if item.author.url else None,
        earning=EarningField(
            to_author=item.reward.to_author, to_voter=item.reward.to_voter
        ),
    )


async def main() -> None:
    await ensure_all_old_collection_indexes()
    logger.info("已为旧版数据构建索引")
    await ensure_all_new_collection_indexes()
    logger.info("已为新版数据构建索引")

    old_data_count = await get_collection_data_count(OldArticleFPRank)
    old_start_time, old_end_time = await get_collection_data_time_range(
        OldArticleFPRank, "date"
    )
    logger.info(
        f"旧集合数据量：{old_data_count}，"
        f"数据时间范围：{old_start_time} - {old_end_time}"
    )

    data_to_save: List[ArticleEarningRankingRecordDocument] = []
    async for item in OldArticleFPRank.find_many(
        sort={"date": "ASC", "ranking": "ASC"}
    ):
        await insert_or_update_user(item)
        data_to_save.append(await convert_item(item))

        if len(data_to_save) == 1000:
            await ArticleEarningRankingRecordDocument.insert_many(data_to_save)
            logger.debug(
                f"已转换 {len(data_to_save)} 条数据，"
                f"最新数据的日期为 {data_to_save[-1].date}"
            )
            data_to_save.clear()

    if len(data_to_save):
        await ArticleEarningRankingRecordDocument.insert_many(data_to_save)
        logger.debug(
            f"已转换 {len(data_to_save)} 条数据，"
            f"最新数据的日期为 {data_to_save[-1].date}"
        )
        data_to_save.clear()

    logger.info("数据转换完成，开始校验")
    new_data_count = await get_collection_data_count(
        ArticleEarningRankingRecordDocument
    )
    if old_data_count != new_data_count:
        logger.critical(
            f"数据量不匹配（迁移前 {old_data_count}，" f"迁移后 {new_data_count}）"
        )
        exit()
    new_start_time, new_end_time = await get_collection_data_time_range(
        ArticleEarningRankingRecordDocument, "date"
    )
    if old_start_time != new_start_time or old_end_time != new_end_time:
        logger.critical(
            "数据时间范围不匹配"
            f"（迁移前 {old_start_time} - {old_end_time}，"
            f"迁移后 {new_start_time} - {new_end_time}）"
        )
        exit()

    logger.info("校验成功，迁移流程结束")


asyncio.run(main())
