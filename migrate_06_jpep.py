import asyncio
from typing import List

from sspeedup.logging.run_logger import RunLogger

from models.jpep.ftn_trade_order import AmountField, FTNTradeOrderDocument
from models.jpep.user import UserDocument
from old_models.jpep_ftn_macket import OldJPEPFTNMacket
from utils.migrate_helper import (
    get_collection_data_count,
)

logger = RunLogger()


async def ensure_all_old_collection_indexes() -> None:
    await OldJPEPFTNMacket.ensure_indexes()


async def ensure_all_new_collection_indexes() -> None:
    await FTNTradeOrderDocument.ensure_indexes()
    await UserDocument.ensure_indexes()


async def insert_or_update_user(item: OldJPEPFTNMacket) -> None:
    await UserDocument.insert_one_if_not_exist(
        updated_at=item.fetch_time,
        id=item.user.id,
        name=item.user.name,
        hashed_name=item.user.name_md5,
    )


async def convert_item(item: OldJPEPFTNMacket) -> FTNTradeOrderDocument:
    return FTNTradeOrderDocument(
        fetch_time=item.fetch_time,
        id=item.order_id,
        published_at=item.publish_time,
        type=item.trade_type,
        price=item.price,
        traded_count=item.transactions_count,
        amount=AmountField(
            total=item.amount.total,
            traded=item.amount.traded,
            tradable=item.amount.tradable,
            minimum_trade=item.minimum_trade_count,
        ),
        publisher_id=item.user.id,
    )


async def main() -> None:
    await ensure_all_old_collection_indexes()
    logger.info("已为旧版数据构建索引")
    await ensure_all_new_collection_indexes()
    logger.info("已为新版数据构建索引")

    old_data_count = await OldJPEPFTNMacket.get_collection().estimated_document_count()
    logger.info(f"旧集合数据量：{old_data_count}")

    data_to_save: List[FTNTradeOrderDocument] = []
    async for item in OldJPEPFTNMacket.find_many(sort={"fetch_time": "ASC"}):
        await insert_or_update_user(item)
        data_to_save.append(await convert_item(item))

        if len(data_to_save) == 5000:
            await FTNTradeOrderDocument.insert_many(data_to_save)
            logger.debug(
                f"已转换 {len(data_to_save)} 条数据，"
                f"最新数据的时间为 {data_to_save[-1].fetch_time}"
            )
            data_to_save.clear()

    if len(data_to_save):
        await FTNTradeOrderDocument.insert_many(data_to_save)
        logger.debug(
            f"已转换 {len(data_to_save)} 条数据，"
            f"最新数据的时间为 {data_to_save[-1].fetch_time}"
        )
        data_to_save.clear()

    logger.info("数据转换完成，开始校验")
    new_data_count = await get_collection_data_count(FTNTradeOrderDocument)
    if old_data_count != new_data_count:
        logger.critical(
            f"数据量不匹配（迁移前 {old_data_count}，" f"迁移后 {new_data_count}）"
        )
        exit()

    logger.info("校验成功，迁移流程结束")


asyncio.run(main())
