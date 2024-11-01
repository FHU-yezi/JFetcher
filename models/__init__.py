from models.jianshu.article_earning_ranking_record import (
    ArticleEarningRankingRecord,
)
from models.jianshu.daily_update_ranking_record import DailyUpdateRankingRecord
from models.jianshu.user import User as JianshuUser
from models.jianshu.user_assets_ranking_record import UserAssetsRankingRecord
from models.jpep.credit_record import CreditRecord
from models.jpep.ftn_trade_order import FTNTradeOrderDocument
from models.jpep.user import User as JPEPUser


async def init_db() -> None:
    await ArticleEarningRankingRecord.init()
    await DailyUpdateRankingRecord.init()
    await JianshuUser.init()
    await UserAssetsRankingRecord.init()
    await CreditRecord.init()
    await FTNTradeOrderDocument.ensure_indexes()
    await JPEPUser.init()
