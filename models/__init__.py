from typing import List, Type, Union

from beanie import Document, View

from models.article_earning_rank_record import ArticleEarningRankRecordModel

MODELS: List[Union[Type[Document], Type[View], str]] = [ArticleEarningRankRecordModel]
