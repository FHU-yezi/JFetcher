from datetime import date, datetime, timedelta
from typing import AsyncGenerator, List

from jkit.article import Article
from jkit.ranking.article_earning import ArticleEarningRanking, RecordField
from jkit.user import User
from prefect import flow, get_run_logger

from models.article_earning_rank_record import (
    ArticleEarningRankRecordModel,
    ArticleField,
    AuthorField,
    EarningField,
)
from utils.db import init_db
from utils.job_model import Job


async def get_user_from_article_slug(article_slug: str) -> User:
    article = Article.from_slug(article_slug)._as_checked()
    article_info = await article.info
    return article_info.author_info.to_user_obj()


async def fetch_data(*, target_date: date) -> AsyncGenerator[RecordField, None]:
    logger = get_run_logger()

    ranking_obj = ArticleEarningRanking(target_date)
    logger.debug(f"已创建文章收益排行榜对象 target_date={ranking_obj._target_date}")

    async for item in ranking_obj:
        yield item


async def process_data(
    item: RecordField, /, *, target_date: date
) -> ArticleEarningRankRecordModel:
    logger = get_run_logger()

    if item.slug:
        author = await get_user_from_article_slug(item.slug)
    else:
        logger.warning(f"文章走丢了，跳过采集文章与作者信息 ranking={item.ranking}")
        author = None

    return ArticleEarningRankRecordModel(
        date=target_date,
        ranking=item.ranking,
        article=ArticleField(
            title=item.title,
            slug=item.slug,
        ),
        author=AuthorField(
            id=(await author.id) if author else None,
            slug=author.slug if author else None,
            name=item.author_info.name,
        ),
        earning=EarningField(
            to_author=item.fp_to_author_anount,
            to_voter=item.fp_to_voter_amount,
        ),
    )


async def save_data(data: List[ArticleEarningRankRecordModel]) -> None:
    await ArticleEarningRankRecordModel.insert_many(data)


@flow
async def main() -> None:
    await init_db([ArticleEarningRankRecordModel])

    target_date = datetime.now().date() - timedelta(days=1)

    data: List[ArticleEarningRankRecordModel] = []
    async for item in fetch_data(target_date=target_date):
        processed_item = await process_data(item, target_date=target_date)
        data.append(processed_item)

    await save_data(data)


fetch_article_earning_rank_records_job = Job(
    func=main,
    name="采集文章收益排行榜记录",
    cron="0 1 * * *",
)
