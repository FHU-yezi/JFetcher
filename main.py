from asyncio import run as asyncio_run

from prefect import serve

from jobs import DEPLOYMENTS
from models import MODELS
from models.new.jianshu.article_earning_ranking_record import (
    ArticleEarningRankingRecord,
)
from models.new.jianshu.daily_update_ranking_record import DailyUpdateRankingRecord
from models.new.jianshu.user import User
from models.new.jianshu.user_assets_ranking_record import UserAssetsRankingRecord
from utils.log import logger


async def main() -> None:
    logger.info("正在为数据库创建索引")
    for model in MODELS:
        await model.ensure_indexes()
    logger.info("索引创建完成")
    logger.info("正在初始化新版数据库")
    await ArticleEarningRankingRecord.init()
    await UserAssetsRankingRecord.init()
    await DailyUpdateRankingRecord.init()
    await User.init()
    logger.info("初始化新版数据库完成")

    logger.info("启动工作流")
    await serve(*DEPLOYMENTS, print_starting_message=False)  # type: ignore


if __name__ == "__main__":
    asyncio_run(main())
