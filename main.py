from asyncio import run as asyncio_run

from prefect import serve

from jobs import DEPLOYMENTS
from models import MODELS
from utils.log import logger


async def main() -> None:
    logger.info("正在为数据库创建索引")
    for model in MODELS:
        await model.ensure_indexes()
    logger.info("索引创建完成")

    logger.info("启动工作流")
    await serve(*DEPLOYMENTS, print_starting_message=False)  # type: ignore


if __name__ == "__main__":
    asyncio_run(main())
