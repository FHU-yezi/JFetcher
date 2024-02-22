from asyncio import run as asyncio_run

from prefect import serve

from jobs import DEPLOYMENTS
from utils.db import init_db
from utils.log import logger


async def main() -> None:
    await init_db()
    logger.info("初始化数据库成功")

    logger.info("启动工作流")
    await serve(*DEPLOYMENTS, print_starting_message=False)  # type: ignore


if __name__ == "__main__":
    asyncio_run(main())
