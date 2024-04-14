import asyncio
from typing import List

from jkit.identifier_convert import user_url_to_slug
from sspeedup.logging.run_logger import RunLogger

from models.jianshu.lottery_win_record import LotteryWinRecordDocument
from models.jianshu.user import UserDocument
from old_models.lottery_data import OldLotteryData
from utils.migrate_helper import (
    get_collection_data_count,
)

logger = RunLogger()


async def ensure_all_old_collection_indexes() -> None:
    await OldLotteryData.ensure_indexes()


async def ensure_all_new_collection_indexes() -> None:
    await UserDocument.ensure_indexes()
    await LotteryWinRecordDocument.ensure_indexes()


async def insert_or_update_user(item: OldLotteryData) -> None:
    if item.user.url:
        await UserDocument.insert_or_update_one(
            slug=user_url_to_slug(item.user.url),
            updated_at=item.time,
            id=item.user.id,
            name=item.user.name,
        )


async def convert_item(item: OldLotteryData) -> LotteryWinRecordDocument:
    return LotteryWinRecordDocument(
        id=item._id,
        time=item.time,
        award_name=item.reward_name,
        user_slug=user_url_to_slug(item.user.url),
    )


async def main() -> None:
    await ensure_all_old_collection_indexes()
    logger.info("已为旧版数据构建索引")
    await ensure_all_new_collection_indexes()
    logger.info("已为新版数据构建索引")

    old_data_count = await get_collection_data_count(OldLotteryData)
    logger.info(f"旧集合数据量：{old_data_count}")

    data_to_save: List[LotteryWinRecordDocument] = []
    async for item in OldLotteryData.find_many(sort={"date": "ASC", "ranking": "ASC"}):
        await insert_or_update_user(item)
        data_to_save.append(await convert_item(item))

        if len(data_to_save) == 1000:
            await LotteryWinRecordDocument.insert_many(data_to_save)
            logger.debug(
                f"已转换 {len(data_to_save)} 条数据，"
                f"最新数据的 ID 为 {data_to_save[-1].id}"
            )
            data_to_save.clear()

    if len(data_to_save):
        await LotteryWinRecordDocument.insert_many(data_to_save)
        logger.debug(
            f"已转换 {len(data_to_save)} 条数据，"
            f"最新数据的 ID 为 {data_to_save[-1].id}"
        )
        data_to_save.clear()

    logger.info("数据转换完成，开始校验")
    new_data_count = await get_collection_data_count(LotteryWinRecordDocument)
    if old_data_count != new_data_count:
        logger.critical(
            f"数据量不匹配（迁移前 {old_data_count}，" f"迁移后 {new_data_count}）"
        )
        exit()

    logger.info("校验成功，迁移流程结束")


asyncio.run(main())
