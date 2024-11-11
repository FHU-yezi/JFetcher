from datetime import date, timedelta
from typing import Optional

from jkit.config import CONFIG
from jkit.exceptions import ResourceUnavailableError
from jkit.ranking.article_earning import ArticleEarningRanking, RecordField
from jkit.user import UserInfo
from prefect import flow
from sshared.retry.asyncio import retry

from models.jianshu.article_earning_ranking_record import ArticleEarningRankingRecord
from models.jianshu.user import User
from utils.log import (
    get_flow_run_name,
    log_flow_run_start,
    log_flow_run_success,
    logger,
)
from utils.prefect_helper import (
    generate_deployment_config,
    generate_flow_config,
)


@retry(attempts=3, delay=5)
async def get_author_slug_and_info(
    item: RecordField,
) -> tuple[Optional[str], Optional[UserInfo]]:
    flow_run_name = get_flow_run_name()

    if not item.slug:
        logger.warn(
            "文章状态异常，跳过作者信息采集",
            flow_run_name=flow_run_name,
            ranking=item.ranking,
        )
        return None, None

    try:
        # TODO: 临时解决简书系统问题数据负数导致的报错
        CONFIG.data_validation.enabled = False
        article_obj = item.to_article_obj()
        author = (await article_obj.info).author_info.to_user_obj()

        author_info = await author.info
        CONFIG.data_validation.enabled = True

        return (author.slug, author_info)
    except ResourceUnavailableError:
        logger.warn(
            "文章或作者状态异常，跳过作者信息采集",
            flow_run_name=flow_run_name,
            ranking=item.ranking,
        )
        return None, None


async def process_item(item: RecordField, date_: date) -> ArticleEarningRankingRecord:
    author_slug, author_info = await get_author_slug_and_info(item)

    if author_slug is not None and author_info is not None:
        await User.upsert(
            slug=author_slug,
            id=author_info.id,
            name=author_info.name,
            avatar_url=author_info.avatar_url,
        )

    return ArticleEarningRankingRecord(
        date=date_,
        ranking=item.ranking,
        slug=item.slug,
        title=item.title,
        author_slug=author_slug,
        author_earning=item.fp_to_author_anount,
        voter_earning=item.fp_to_voter_amount,
    )


@flow(
    **generate_flow_config(
        name="采集文章收益排行榜记录",
    )
)
async def main() -> None:
    log_flow_run_start(logger)

    date_ = date.today() - timedelta(days=1)

    data: list[ArticleEarningRankingRecord] = []
    async for item in ArticleEarningRanking(date_):
        processed_item = await process_item(item, date_=date_)
        data.append(processed_item)

    await ArticleEarningRankingRecord.insert_many(data)

    log_flow_run_success(logger, data_count=len(data))


deployment = main.to_deployment(
    **generate_deployment_config(
        name="采集文章收益排行榜记录",
        cron="0 1 * * *",
    )
)
