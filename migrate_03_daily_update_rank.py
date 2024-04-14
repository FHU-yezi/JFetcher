import asyncio
from datetime import datetime
from typing import List

from jkit.identifier_convert import user_url_to_slug
from sspeedup.logging.run_logger import RunLogger

from models.jianshu.daily_update_ranking_record import DailyUpdateRankingRecordDocument
from models.jianshu.user import UserDocument
from old_models.daily_update_rank import OldDailyUpdateRank
from utils.migrate_helper import (
    get_collection_data_count,
    get_collection_data_time_range,
)

logger = RunLogger()


async def ensure_all_old_collection_indexes() -> None:
    await OldDailyUpdateRank.ensure_indexes()


async def ensure_all_new_collection_indexes() -> None:
    await UserDocument.ensure_indexes()
    await DailyUpdateRankingRecordDocument.ensure_indexes()


async def insert_or_update_user(item: OldDailyUpdateRank) -> None:
    if item.user.url:
        await UserDocument.insert_or_update_one(
            slug=user_url_to_slug(item.user.url),
            updated_at=datetime.fromisoformat(item.date.isoformat()),
            name=item.user.name,
        )


async def convert_item(item: OldDailyUpdateRank) -> DailyUpdateRankingRecordDocument:
    return DailyUpdateRankingRecordDocument(
        date=item.date,
        ranking=item.ranking,
        days=item.days,
        user_slug=user_url_to_slug(item.user.url),
    )


async def main() -> None:
    await ensure_all_old_collection_indexes()
    logger.info("已为旧版数据构建索引")
    await ensure_all_new_collection_indexes()
    logger.info("已为新版数据构建索引")

    old_data_count = await get_collection_data_count(OldDailyUpdateRank)
    old_start_time, old_end_time = await get_collection_data_time_range(
        OldDailyUpdateRank, "date"
    )
    logger.info(
        f"旧集合数据量：{old_data_count}，"
        f"数据时间范围：{old_start_time} - {old_end_time}"
    )

    data_to_save: List[DailyUpdateRankingRecordDocument] = []
    async for item in OldDailyUpdateRank.find_many(
        sort={"date": "ASC", "ranking": "ASC"}
    ):
        await insert_or_update_user(item)
        data_to_save.append(await convert_item(item))

        if len(data_to_save) == 1000:
            await DailyUpdateRankingRecordDocument.insert_many(data_to_save)
            logger.debug(
                f"已转换 {len(data_to_save)} 条数据，"
                f"最新数据的日期为 {data_to_save[-1].date}"
            )
            data_to_save.clear()

    if len(data_to_save):
        await DailyUpdateRankingRecordDocument.insert_many(data_to_save)
        logger.debug(
            f"已转换 {len(data_to_save)} 条数据，"
            f"最新数据的日期为 {data_to_save[-1].date}"
        )
        data_to_save.clear()

    logger.info("数据转换完成，开始校验")
    new_data_count = await get_collection_data_count(DailyUpdateRankingRecordDocument)
    if old_data_count != new_data_count:
        logger.critical(
            f"数据量不匹配（迁移前 {old_data_count}，" f"迁移后 {new_data_count}）"
        )
        exit()
    new_start_time, new_end_time = await get_collection_data_time_range(
        DailyUpdateRankingRecordDocument, "date"
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
