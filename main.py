from asyncio import run as asyncio_run

from prefect import serve

from jobs import DEPLOYMENTS
from models.jianshu.article_earning_ranking_record import (
    ArticleEarningRankingRecordDocument,
)
from models.jianshu.assets_ranking_record import AssetsRankingRecordDocument
from models.jianshu.daily_update_ranking_record import DailyUpdateRankingRecordDocument
from models.jianshu.lottery_win_record import LotteryWinRecordDocument
from models.jianshu.lp_recommend_article_record import (
    LPRecommendedArticleRecordDocument,
)
from models.jianshu.user import UserDocument as JianshuUserDocument
from models.jpep.credit_history import CreditHistoryDocument
from models.jpep.ftn_trade_order import FTNTradeOrderDocument
from models.jpep.user import UserDocument as JPEPUserDocument
from utils.log import logger


async def main() -> None:
    logger.info("正在为数据库创建索引...")
    await ArticleEarningRankingRecordDocument.ensure_indexes()
    await AssetsRankingRecordDocument.ensure_indexes()
    await DailyUpdateRankingRecordDocument.ensure_indexes()
    await LotteryWinRecordDocument.ensure_indexes()
    await LPRecommendedArticleRecordDocument.ensure_indexes()
    await JianshuUserDocument.ensure_indexes()
    await CreditHistoryDocument.ensure_indexes()
    await FTNTradeOrderDocument.ensure_indexes()
    await JPEPUserDocument.ensure_indexes()
    logger.info("索引创建完成")

    logger.info("启动工作流...")
    await serve(*DEPLOYMENTS, print_starting_message=False)  # type: ignore


if __name__ == "__main__":
    asyncio_run(main())
