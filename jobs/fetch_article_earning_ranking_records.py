from datetime import date, datetime, timedelta
from typing import List, Optional

from bson import ObjectId
from jkit._constraints import PositiveFloat, PositiveInt
from jkit.article import Article
from jkit.config import CONFIG
from jkit.ranking.article_earning import ArticleEarningRanking, RecordField
from jkit.user import User
from prefect import flow, get_run_logger
from prefect.states import Completed, State

from utils.db import DB
from utils.document_model import (
    DOCUMENT_OBJECT_CONFIG,
    FIELD_OBJECT_CONFIG,
    Documemt,
    Field,
)
from utils.job_model import Job

COLLECTION = DB.article_earning_ranking_records


class ArticleField(Field, **FIELD_OBJECT_CONFIG):
    title: Optional[str]
    slug: Optional[str]


class AuthorField(Field, **FIELD_OBJECT_CONFIG):
    id: Optional[PositiveInt]
    slug: Optional[str]
    name: Optional[str]


class EarningField(Field, **FIELD_OBJECT_CONFIG):
    to_author: PositiveFloat
    to_voter: PositiveFloat


class ArticleEarningRankingRecordDocument(Documemt, **DOCUMENT_OBJECT_CONFIG):
    date: date
    ranking: PositiveInt
    article: ArticleField
    author: AuthorField
    earning: EarningField


async def get_article_author(article_slug: str, /) -> User:
    article = Article.from_slug(article_slug)._as_checked()
    article_info = await article.info
    return article_info.author_info.to_user_obj()


async def process_item(
    item: RecordField, /, *, target_date: date
) -> ArticleEarningRankingRecordDocument:
    logger = get_run_logger()

    if item.slug:
        # TODO: 临时解决简书系统问题数据负数导致的报错
        CONFIG.data_validation.enabled = False
        author = await get_article_author(item.slug)
        author_id = await author.id
        author_slug = author.slug
        CONFIG.data_validation.enabled = True
    else:
        logger.warning(f"文章走丢了，跳过采集文章与作者信息 ranking={item.ranking}")
        author_id = None
        author_slug = None

    return ArticleEarningRankingRecordDocument(
        _id=ObjectId(),
        date=target_date,
        ranking=item.ranking,
        article=ArticleField(
            title=item.title,
            slug=item.slug,
        ),
        author=AuthorField(
            id=author_id,
            slug=author_slug,
            name=item.author_info.name,
        ),
        earning=EarningField(
            to_author=item.fp_to_author_anount,
            to_voter=item.fp_to_voter_amount,
        ),
    )


@flow
async def main() -> State:
    target_date = datetime.now().date() - timedelta(days=1)

    data: List[ArticleEarningRankingRecordDocument] = []
    async for item in ArticleEarningRanking(target_date):
        processed_item = await process_item(item, target_date=target_date)
        data.append(processed_item)

    await COLLECTION.insert_many(x.to_dict() for x in data)

    return Completed(message=f"target_date={target_date}, data_count={len(data)}")


fetch_article_earning_ranking_records_job = Job(
    func=main,
    name="采集文章收益排行榜记录",
    cron="0 1 * * *",
)
