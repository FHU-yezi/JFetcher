from asyncio import run as asyncio_run

from prefect import serve

from jobs import DEPLOYMENTS
from models.jianshu.article_earning_ranking_record import (
    ArticleEarningRankingRecord,
)
from models.jianshu.daily_update_ranking_record import DailyUpdateRankingRecord
from models.jianshu.user import User
from models.jianshu.user_assets_ranking_record import UserAssetsRankingRecord
from models.jpep.credit_history import CreditHistoryDocument
from models.jpep.ftn_trade_order import FTNTradeOrderDocument
from models.jpep.user import UserDocument
from utils.log import logger


async def main() -> None:
    await CreditHistoryDocument.ensure_indexes()
    await FTNTradeOrderDocument.ensure_indexes()
    await UserDocument.ensure_indexes()

    await ArticleEarningRankingRecord.init()
    await UserAssetsRankingRecord.init()
    await DailyUpdateRankingRecord.init()
    await User.init()
    logger.info("初始化数据库完成")

    logger.info("启动工作流")
    await serve(*DEPLOYMENTS, print_starting_message=False)  # type: ignore


if __name__ == "__main__":
    asyncio_run(main())
