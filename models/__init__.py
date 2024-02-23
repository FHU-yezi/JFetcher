from typing import List, Type

from beanie import Document

from models.article_earning_rank_record import ArticleEarningRankRecordModel

MODELS: List[Type[Document]] = [ArticleEarningRankRecordModel]
