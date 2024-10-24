from sshared.mongo import Document

from .jianshu.article_earning_ranking_record import ArticleEarningRankingRecordDocument
from .jianshu.assets_ranking_record import AssetsRankingRecordDocument
from .jianshu.daily_update_ranking_record import DailyUpdateRankingRecordDocument
from .jianshu.user import UserDocument as JianshuUserDocument
from .jpep.credit_history import CreditHistoryDocument
from .jpep.ftn_trade_order import FTNTradeOrderDocument
from .jpep.user import UserDocument as JPEPUserDocument

MODELS: tuple[type[Document], ...] = (
    ArticleEarningRankingRecordDocument,
    AssetsRankingRecordDocument,
    DailyUpdateRankingRecordDocument,
    JianshuUserDocument,
    CreditHistoryDocument,
    FTNTradeOrderDocument,
    JPEPUserDocument,
)
