from asyncio import run as asyncio_run

from sshared.logging import Logger

from models.jianshu.user import UserDocument
from models.new.jianshu.user import StatusEnum, User

logger = Logger()


async def main() -> None:
    await User.init()
    logger.debug("初始化数据库表完成")

    async for item in UserDocument.Meta.collection.find().sort({"slug": 1}):
        await User(
            slug=item["slug"],
            status={
                "NORMAL": StatusEnum.NORMAL,
                "INACCESSIBLE": StatusEnum.INACCESSIBLE,
            }[item["status"]],
            id=item["id"],
            name=item["name"],
            update_time=item["updatedAt"],
            history_names=item["historyNames"],
            avatar_url=item["avatarUrl"],
        ).create()
        logger.debug(f"迁移 {item['slug']} 完成")

    logger.info("数据迁移完成")


asyncio_run(main())
