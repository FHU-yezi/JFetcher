from asyncio import run as asyncio_run

from sshared.logging import Logger

from models.jpep.new.ftn_order import FTNOrder, TypeEnum
from utils.mongo import JPEP_DB

OLD_COLLECTION = JPEP_DB.ftn_trade_orders
logger = Logger()


async def main() -> None:
    await FTNOrder.init()

    logger.info("开始执行数据迁移")
    for order_id in await OLD_COLLECTION.distinct("id"):
        last_record = await OLD_COLLECTION.find_one(
            {"id": order_id}, sort={"fetchTime": -1}
        )
        if not last_record:
            raise ValueError

        await FTNOrder(
            id=last_record["id"],
            type={"buy": TypeEnum.BUY, "sell": TypeEnum.SELL}[last_record["type"]],
            publisher_id=last_record["publisherId"],
            publish_time=last_record["publishedAt"],
            last_seen_time=last_record["fetchTime"],
        ).create()
        logger.debug(f"已迁移订单 {order_id}")

    logger.info("数据迁移完成")


asyncio_run(main())
