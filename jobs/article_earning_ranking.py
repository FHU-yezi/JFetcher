from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

from jkit.config import CONFIG
from jkit.exceptions import ResourceUnavailableError
from jkit.ranking.article_earning import ArticleEarningRanking, RecordField
from prefect import flow, get_run_logger
from prefect.states import Completed, State

from models.article_earning_ranking_record import (
    ArticleEarningRankingRecordDocument,
    ArticleField,
    EarningField,
    insert_many,
)
from models.article_earning_ranking_record import (
    init_db as init_article_earning_ranking_record_db,
)
from models.jianshu_user import init_db as init_jianshu_user_db
from models.jianshu_user import insert_or_update_one
from utils.async_retry import async_retry
from utils.config_generators import (
    generate_deployment_config,
    generate_flow_config,
)


@async_retry()
async def get_author_id_and_slug(
    item: RecordField, /
) -> Tuple[Optional[int], Optional[str]]:
    logger = get_run_logger()

    if not item.slug:
        logger.warning(f"文章走丢了，跳过采集文章与作者信息 ranking={item.ranking}")
        return None, None

    try:
        # TODO: 临时解决简书系统问题数据负数导致的报错
        CONFIG.data_validation.enabled = False
        article_obj = item.to_article_obj()
        author = (await article_obj.info).author_info.to_user_obj()

        result = (await author.id, author.slug)
        CONFIG.data_validation.enabled = True

        return result
    except ResourceUnavailableError:
        logger.warning(
            f"文章或作者状态异常，跳过采集文章与作者信息 ranking={item.ranking}"
        )
        return None, None


async def process_item(
    item: RecordField, /, *, target_date: date
) -> ArticleEarningRankingRecordDocument:
    author_id, author_slug = await get_author_id_and_slug(item)

    if author_slug:
        await insert_or_update_one(
            slug=author_slug,
            id=author_id,
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
async def flow_func() -> State:
    await init_article_earning_ranking_record_db()
    await init_jianshu_user_db()

    target_date = datetime.now().date() - timedelta(days=1)

    data: List[ArticleEarningRankingRecordDocument] = []
    async for item in ArticleEarningRanking(target_date):
        processed_item = await process_item(item, target_date=target_date)
        data.append(processed_item)

    await insert_many(data)

    return Completed(message=f"target_date={target_date}, data_count={len(data)}")


deployment = flow_func.to_deployment(
    **generate_deployment_config(
        name="采集文章收益排行榜记录",
        cron="0 1 * * *",
    )
)
