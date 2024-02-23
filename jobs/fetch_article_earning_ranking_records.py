from datetime import date, datetime, timedelta
from typing import List, Optional

from beanie import Document
from jkit.article import Article
from jkit.ranking.article_earning import ArticleEarningRanking, RecordField
from jkit.user import User
from prefect import flow, get_run_logger
from pydantic import BaseModel, Field, PastDate, PositiveFloat, PositiveInt

from utils.db import init_db
from utils.job_model import Job


class ArticleField(BaseModel):
    title: Optional[str]
    slug: Optional[str]


class AuthorField(BaseModel):
    id: Optional[PositiveInt]
    slug: Optional[str]
    name: Optional[str]


class EarningField(BaseModel):
    to_author: PositiveFloat = Field(serialization_alias="toAuthor")
    to_voter: PositiveFloat = Field(serialization_alias="toVoter")


class ArticleEarningRankingRecordModel(Document):
    date: PastDate
    ranking: PositiveInt
    article: ArticleField
    author: AuthorField
    earning: EarningField

    class Settings:
        name = "article_earning_ranking_record"
        indexes = ("date", "ranking")


async def get_article_author(article_slug: str, /) -> User:
    article = Article.from_slug(article_slug)._as_checked()
    article_info = await article.info
    return article_info.author_info.to_user_obj()


async def process_item(
    item: RecordField, /, *, target_date: date
) -> ArticleEarningRankingRecordModel:
    logger = get_run_logger()

    if item.slug:
        author = await get_article_author(item.slug)
    else:
        logger.warning(f"文章走丢了，跳过采集文章与作者信息 ranking={item.ranking}")
        author = None

    return ArticleEarningRankingRecordModel(
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


@flow
async def main() -> None:
    logger = get_run_logger()

    await init_db([ArticleEarningRankingRecordModel])
    logger.info("初始化 ODM 模型成功")

    target_date = datetime.now().date() - timedelta(days=1)
    logger.info(f"target_date={target_date}")

    data: List[ArticleEarningRankingRecordModel] = []
    async for item in ArticleEarningRanking(target_date):
        processed_item = await process_item(item, target_date=target_date)
        data.append(processed_item)

    await ArticleEarningRankingRecordModel.insert_many(data)


fetch_article_earning_ranking_records_job = Job(
    func=main,
    name="采集文章收益排行榜记录",
    cron="0 1 * * *",
)
