from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import date, datetime, timedelta

from jkit.config import CONFIG as JKIT_CONFIG
from jkit.ranking.article_earning import ArticleEarningRanking, RecordData
from jkit.user import InfoData as UserInfoData
from prefect import flow, get_run_logger, task
from prefect.states import Completed, Failed, State
from sshared.retry import retry

from models.jianshu.article_earning_ranking_record import ArticleEarningRankingRecord
from models.jianshu.user import User
from utils.config import CONFIG
from utils.prefect_helper import get_flow_run_name, get_task_run_name
from utils.retry import get_network_request_retry_params

JKIT_CONFIG.data_validation.enabled = False
if CONFIG.jianshu_endpoint:
    JKIT_CONFIG.datasources.jianshu.endpoint = CONFIG.jianshu_endpoint


@retry(**get_network_request_retry_params())
async def get_article_author_info(
    item: RecordData,
) -> UserInfoData:
    article = item.to_article_obj()
    article_info = await article.info
    author = article_info.author_info.to_user_obj()

    return await author.info


@task(task_run_name=get_task_run_name)
async def iter_article_earning_ranking(date: date) -> AsyncGenerator[RecordData]:
    async for item in ArticleEarningRanking(date).iter_records():
        yield item


@task(task_run_name=get_task_run_name)
async def save_data_to_db(data: list[ArticleEarningRankingRecord]) -> None:
    await ArticleEarningRankingRecord.insert_many(data)


@flow(
    name="采集简书文章收益排行榜数据",
    flow_run_name=get_flow_run_name,
    retries=2,
    retry_delay_seconds=300,
    timeout_seconds=300,
)
async def jianshu_fetch_article_earning_ranking_data(date: date | None = None) -> State:
    logger = get_run_logger()

    if not date:
        date = datetime.now().date() - timedelta(days=1)

    if await ArticleEarningRankingRecord.is_records_exist(date):
        logger.error("该日期的数据已存在 date=%s", date)
        return Failed()

    data: list[ArticleEarningRankingRecord] = []
    async for item in iter_article_earning_ranking(date):
        if not item.slug:
            logger.warning("文章状态异常，跳过作者数据采集 ranking=%s", item.ranking)

            data.append(
                ArticleEarningRankingRecord(
                    date=date,
                    ranking=item.ranking,
                    slug=None,
                    title=None,
                    author_slug=None,
                    author_earning=item.fp_to_author_anount,
                    voter_earning=item.fp_to_voter_amount,
                )
            )
            continue

        author_info = await get_article_author_info(item)

        await User.upsert(
            slug=author_info.slug,
            id=author_info.id,
            name=author_info.name,
            avatar_url=author_info.avatar_url,
        )

        data.append(
            ArticleEarningRankingRecord(
                date=date,
                ranking=item.ranking,
                slug=item.slug,
                title=item.title,
                author_slug=author_info.slug,
                author_earning=item.fp_to_author_anount,
                voter_earning=item.fp_to_voter_amount,
            )
        )

    await save_data_to_db(data)

    return Completed()
