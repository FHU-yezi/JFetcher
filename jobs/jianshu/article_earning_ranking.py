from datetime import datetime, timedelta
from typing import Optional

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
from models.new.jianshu.article_earning_ranking_record import (
    ArticleEarningRankingRecord as NewDbArticleEarningRankingRecord,
)
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


@retry(attempts=5, delay=10)
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


async def process_item(
    item: RecordField, date: datetime
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
        date=date,
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
    )


def transform_to_new_db_model(
    data: list[ArticleEarningRankingRecordDocument],
) -> list[NewDbArticleEarningRankingRecord]:
    result: list[NewDbArticleEarningRankingRecord] = []
    for item in data:
        result.append(  # noqa: PERF401
            NewDbArticleEarningRankingRecord(
                date=item.date.date(),
                ranking=item.ranking,
                article_slug=item.article.slug,
                article_title=item.article.title,
                author_slug=item.author_slug,
                earning_to_author=item.earning.to_author,
                earning_to_voter=item.earning.to_voter,
            )
        )

    return result


@flow(
    **generate_flow_config(
        name="采集文章收益排行榜记录",
    )
)
async def main() -> None:
    log_flow_run_start(logger)

    date = get_today_as_datetime() - timedelta(days=1)

    data: list[ArticleEarningRankingRecordDocument] = []
    async for item in ArticleEarningRanking(date.date()):
        processed_item = await process_item(item, date=date)
        data.append(processed_item)

    await ArticleEarningRankingRecordDocument.insert_many(data)

    new_data = transform_to_new_db_model(data)
    await NewDbArticleEarningRankingRecord.insert_many(new_data)

    log_flow_run_success(logger, data_count=len(data))


deployment = main.to_deployment(
    **generate_deployment_config(
        name="采集文章收益排行榜记录",
        cron="0 1 * * *",
    )
)
