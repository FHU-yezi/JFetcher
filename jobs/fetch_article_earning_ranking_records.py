from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

from jkit._constraints import PositiveFloat, PositiveInt
from jkit.config import CONFIG
from jkit.exceptions import ResourceUnavailableError
from jkit.ranking.article_earning import ArticleEarningRanking, RecordField
from prefect import flow, get_run_logger
from prefect.states import Completed, State
from pymongo import IndexModel

from utils.config_generators import (
    generate_deployment_config,
    generate_flow_config,
)
from utils.db import DB
from utils.document_model import (
    DOCUMENT_OBJECT_CONFIG,
    FIELD_OBJECT_CONFIG,
    Documemt,
    Field,
)

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


async def init_db() -> None:
    await COLLECTION.create_indexes([IndexModel([("date", "ranking")], unique=True)])


async def process_item(
    item: RecordField, /, *, target_date: date
) -> ArticleEarningRankingRecordDocument:
    author_id, author_slug = await get_author_id_and_slug(item)

    return ArticleEarningRankingRecordDocument(
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
    ).validate()


@flow(
    **generate_flow_config(
        name="采集文章收益排行榜记录",
    )
)
async def flow_func() -> State:
    await init_db()

    target_date = datetime.now().date() - timedelta(days=1)

    data: List[ArticleEarningRankingRecordDocument] = []
    async for item in ArticleEarningRanking(target_date):
        processed_item = await process_item(item, target_date=target_date)
        data.append(processed_item)

    await COLLECTION.insert_many(x.to_dict() for x in data)

    return Completed(message=f"target_date={target_date}, data_count={len(data)}")


deployment = flow_func.to_deployment(
    **generate_deployment_config(
        name="采集文章收益排行榜记录",
        cron="0 1 * * *",
    )
)
