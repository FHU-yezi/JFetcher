from asyncio import run as asyncio_run

from sshared.logging import Logger

from models.jpep.credit_record import CreditRecord
from utils.mongo import JPEP_DB

OLD_COLLECTION = JPEP_DB.credit_history
logger = Logger()


async def main() -> None:
    await CreditRecord.init()

    logger.info("开始执行数据迁移")
    async for item in OLD_COLLECTION.find().sort({"time": 1, "userId": 1}):
        await CreditRecord(
            time=item["time"], user_id=item["userId"], credit=item["value"]
        ).create()

        logger.debug(f"已迁移 {item['time']} - {item['userId']}")

    logger.info("数据迁移完成")


asyncio_run(main())
