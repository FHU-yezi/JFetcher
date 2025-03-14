from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import date, datetime, timedelta

from jkit.config import CONFIG as JKIT_CONFIG
from jkit.ranking.article_earning import ArticleEarningRanking, RecordData
from jkit.user import InfoData as UserInfoData
from prefect import flow, get_run_logger, task
from sshared.retry import retry

from models.jianshu.article_earning_ranking_record import ArticleEarningRankingRecord
from models.jianshu.user import User
from utils.config import CONFIG
from utils.exceptions import DataExistsError
from utils.prefect_helper import get_flow_run_name, get_task_run_name
from utils.retry import NETWORK_REQUEST_RETRY_PARAMS

TOTAL_DATA_COUNT = 100

JKIT_CONFIG.data_validation.enabled = False
if CONFIG.jianshu_endpoint:
    JKIT_CONFIG.datasources.jianshu.endpoint = CONFIG.jianshu_endpoint


@retry(**NETWORK_REQUEST_RETRY_PARAMS)
async def get_article_author_info(
    item: RecordData,
) -> UserInfoData:
    article = item.to_article_obj()
    article_info = await article.info
    author = article_info.author_info.to_user_obj()

    return await author.info


@task(task_run_name=get_task_run_name)
async def pre_check(*, date: date) -> int:
    logger = get_run_logger()

    current_data_count = await ArticleEarningRankingRecord.count_by_date(date)
    if current_data_count == TOTAL_DATA_COUNT:
        raise DataExistsError(f"该日期的数据已存在 {date=}")
    if 0 < current_data_count < TOTAL_DATA_COUNT:
        logger.warning("正在进行断点续采 current_data_count=%s", current_data_count)

    return current_data_count + 1


@task(task_run_name=get_task_run_name)
async def iter_article_earning_ranking(date: date) -> AsyncGenerator[RecordData]:
    async for item in ArticleEarningRanking(date_=date).iter_records():
        yield item


async def save_user_data(*, author_info: UserInfoData) -> None:
    user = await User.get_by_slug(author_info.slug)
    if not user:
        await User.create(
            slug=author_info.slug,
            id=author_info.id,
            name=author_info.name,
            avatar_url=author_info.avatar_url,
            membership_type=author_info.membership_info.type,
            membership_expire_time=author_info.membership_info.expire_time,
        )
    else:
        await User.update_by_slug(
            slug=author_info.slug,
            name=author_info.name,
            avatar_url=author_info.avatar_url,
            membership_type=author_info.membership_info.type,
            membership_expire_time=author_info.membership_info.expire_time,
        )


async def save_article_earning_ranking_record_data(
    item: RecordData, /, *, date: date, author_info: UserInfoData | None
) -> None:
    await ArticleEarningRankingRecord.create(
        date=date,
        ranking=item.ranking,
        slug=item.slug,
        title=item.title,
        author_slug=author_info.slug if author_info else None,
        author_earning=item.fp_to_author_amount,
        voter_earning=item.fp_to_voter_amount,
    )


@flow(
    name="采集简书文章收益排行榜数据",
    flow_run_name=get_flow_run_name,
    retries=2,
    retry_delay_seconds=300,
    timeout_seconds=300,
)
async def jianshu_fetch_article_earning_ranking_data(date: date | None = None) -> None:
    logger = get_run_logger()

    if not date:
        date = datetime.now().date() - timedelta(days=1)

    start_ranking = await pre_check(date=date)

    async for item in iter_article_earning_ranking(date):
        # 断点续采
        if item.ranking < start_ranking:
            continue

        if item.slug:
            author_info: UserInfoData = await get_article_author_info(item)

            try:
                await save_user_data(author_info=author_info)
            except Exception:
                logger.exception("保存用户数据时发生未知异常")
        else:
            # TODO
            author_info = None  # type: ignore
            logger.warning("文章状态异常，跳过作者数据采集 ranking=%s", item.ranking)

        try:
            await save_article_earning_ranking_record_data(
                item, date=date, author_info=author_info
            )
        except Exception:
            logger.exception("保存文章收益排行榜数据时发生未知异常 id=%s", item.ranking)
