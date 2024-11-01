from asyncio import run as asyncio_run

from sshared.logging import Logger

from models.jpep.user import User
from utils.mongo import JPEP_DB

OLD_COLLECTION = JPEP_DB.users
logger = Logger()


async def main() -> None:
    await User.init()

    logger.info("开始执行数据迁移")
    async for item in OLD_COLLECTION.find().sort({"id": 1}):
        await User(
            id=item["id"],
            update_time=item["updatedAt"],
            name=item["name"],
            hashed_name=item["hashedName"],
            avatar_url=item["avatarUrl"],
        ).create()

        logger.debug(f"已迁移 {item['id']}")

    logger.info("数据迁移完成")


asyncio_run(main())
