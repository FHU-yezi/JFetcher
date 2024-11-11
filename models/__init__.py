from models.jianshu.article_earning_ranking_record import (
    ArticleEarningRankingRecord,
)
from models.jianshu.daily_update_ranking_record import DailyUpdateRankingRecord
from models.jianshu.user import User as JianshuUser
from models.jianshu.user_assets_ranking_record import UserAssetsRankingRecord
from models.jpep.credit_record import CreditRecord
from models.jpep.ftn_macket_record import FTNMacketRecord
from models.jpep.ftn_order import FTNOrder
from models.jpep.user import User as JPEPUser
from utils.db import jianshu_pool, jpep_pool


async def init_db() -> None:
    await jianshu_pool.prepare()
    await jpep_pool.prepare()

    await ArticleEarningRankingRecord.init()
    await DailyUpdateRankingRecord.init()
    await JianshuUser.init()
    await UserAssetsRankingRecord.init()
    await CreditRecord.init()
    await FTNMacketRecord.init()
    await FTNOrder.init()
    await JPEPUser.init()
