from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from jkit.config import CONFIG
from jkit.exceptions import ResourceUnavailableError
from jkit.ranking.article_earning import ArticleEarningRanking, RecordField
from jkit.user import UserInfo
from prefect import flow
from sshared.retry.asyncio import retry
from sshared.time import get_today_as_datetime

from models.jianshu.article_earning_ranking_record import (
    ArticleEarningRankingRecordDocument,
    ArticleField,
    EarningField,
)
from models.jianshu.user import UserDocument
from utils.log import log_flow_run_start, log_flow_run_success, logger
from utils.prefect_helper import (
    generate_deployment_config,
    generate_flow_config,
)


@retry(attempts=5, delay=10)
async def get_author_slug_and_info(
    item: RecordField, /
) -> Tuple[Optional[str], Optional[UserInfo]]:
    if not item.slug:
        logger.warn(f"文章走丢了，跳过采集作者信息 ranking={item.ranking}")
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
        logger.warn(f"文章或作者状态异常，跳过采集作者信息 ranking={item.ranking}")
        return None, None


async def process_item(
    item: RecordField, /, *, target_date: datetime
) -> ArticleEarningRankingRecordDocument:
    author_slug, author_info = await get_author_slug_and_info(item)

    if author_slug is not None and author_info is not None:
        await UserDocument.insert_or_update_one(
            slug=author_slug,
            id=author_info.id,
            name=author_info.name,
            avatar_url=author_info.avatar_url,
        )

    return ArticleEarningRankingRecordDocument(
        date=target_date,
        ranking=item.ranking,
        article=ArticleField(
            title=item.title,
            slug=item.slug,
        ),
        author_slug=author_slug,
        earning=EarningField(
            to_author=item.fp_to_author_anount,
            to_voter=item.fp_to_voter_amount,
        ),
    ).validate()


@flow(
    **generate_flow_config(
        name="采集文章收益排行榜记录",
    )
)
async def main() -> None:
    log_flow_run_start(logger)

    target_date = get_today_as_datetime() - timedelta(days=1)

    data: List[ArticleEarningRankingRecordDocument] = []
    async for item in ArticleEarningRanking(target_date.date()):
        processed_item = await process_item(item, target_date=target_date)
        data.append(processed_item)

    await ArticleEarningRankingRecordDocument.insert_many(data)

    log_flow_run_success(logger, data_count=len(data))


deployment = main.to_deployment(
    **generate_deployment_config(
        name="采集文章收益排行榜记录",
        cron="0 1 * * *",
    )
)
