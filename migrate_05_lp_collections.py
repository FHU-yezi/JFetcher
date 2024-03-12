import asyncio
from typing import List

from jkit.identifier_convert import article_url_to_slug, user_url_to_slug
from sspeedup.logging.run_logger import RunLogger

from models.jianshu.lp_recommend_article_record import (
    LPRecommendedArticleRecordDocument,
)
from models.jianshu.user import JianshuUserDocument
from old_models.lp_collections import OldLPCollections
from utils.migrate_helper import (
    get_collection_data_count,
    get_collection_data_time_range,
)

logger = RunLogger()


async def ensure_all_old_collection_indexes() -> None:
    await OldLPCollections.ensure_indexes()


async def ensure_all_new_collection_indexes() -> None:
    await JianshuUserDocument.ensure_indexes()
    await LPRecommendedArticleRecordDocument.ensure_indexes()


async def insert_or_update_user(item: OldLPCollections) -> None:
    await JianshuUserDocument.insert_or_update_one(
        slug=user_url_to_slug(item.author.url),
        updated_at=item.fetch_date,
        id=item.author.id,
        name=item.author.name,
    )


async def convert_item(item: OldLPCollections) -> LPRecommendedArticleRecordDocument:
    return LPRecommendedArticleRecordDocument(
        date=item.fetch_date,
        id=item.article.id,
        slug=article_url_to_slug(item.article.url),
        title=item.article.title,
        published_at=item.article.release_time,
        views_count=item.article.views_count,
        likes_count=item.article.likes_count,
        comments_count=item.article.comments_count,
        tips_count=item.article.rewards_count,
        earned_fp_amount=item.article.total_fp_amount,
        is_paid=item.article.is_paid,
        can_comment=item.article.is_commentable,
        description=item.article.summary,
        author_slug=user_url_to_slug(item.author.url),
    )


async def main() -> None:
    await ensure_all_old_collection_indexes()
    logger.info("已为旧版数据构建索引")
    await ensure_all_new_collection_indexes()
    logger.info("已为新版数据构建索引")

    old_data_count = await get_collection_data_count(OldLPCollections)
    old_start_time, old_end_time = await get_collection_data_time_range(
        OldLPCollections, "date"
    )
    logger.info(
        f"旧集合数据量：{old_data_count}，"
        f"数据时间范围：{old_start_time} - {old_end_time}"
    )

    data_to_save: List[LPRecommendedArticleRecordDocument] = []
    async for item in OldLPCollections.Meta.collection.find().sort(
        {"date": 1, "ranking": 1}
    ):
        item = OldLPCollections.from_dict(item)
        await insert_or_update_user(item)
        data_to_save.append(await convert_item(item))

        if len(data_to_save) == 1000:
            await LPRecommendedArticleRecordDocument.insert_many(data_to_save)
            logger.debug(
                f"已转换 {len(data_to_save)} 条数据，"
                f"最新数据的日期为 {data_to_save[-1].date}"
            )
            data_to_save.clear()

    if len(data_to_save):
        await LPRecommendedArticleRecordDocument.insert_many(data_to_save)
        logger.debug(
            f"已转换 {len(data_to_save)} 条数据，"
            f"最新数据的日期为 {data_to_save[-1].date}"
        )
        data_to_save.clear()

    logger.info("数据转换完成，开始校验")
    new_data_count = await get_collection_data_count(LPRecommendedArticleRecordDocument)
    if old_data_count != new_data_count:
        logger.critical(
            f"数据量不匹配（迁移前 {old_data_count}，" f"迁移后 {new_data_count}）"
        )
        exit()
    new_start_time, new_end_time = await get_collection_data_time_range(
        LPRecommendedArticleRecordDocument, "date"
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
