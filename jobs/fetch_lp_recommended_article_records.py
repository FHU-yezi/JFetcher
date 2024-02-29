from datetime import date, datetime
from typing import List, Optional

from jkit._constraints import (
    ArticleSlug,
    NonEmptyStr,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveInt,
)
from jkit.collection import Collection, CollectionArticleInfo
from msgspec import field
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

COLLECTION = DB.lp_recommended_article_records

# 理事会点赞汇总专题
LP_RECOMMENDED_COLLECTION = Collection.from_slug("f61832508891")


class AuthorInfoField(Field, **FIELD_OBJECT_CONFIG):
    id: PositiveInt
    slug: str
    name: str


class LPRecommendedArticleRecord(Documemt, **DOCUMENT_OBJECT_CONFIG):
    date: date
    id: PositiveInt
    slug: ArticleSlug
    title: NonEmptyStr
    published_at: datetime

    views_count: NonNegativeInt
    likes_count: NonNegativeInt
    comments_count: NonNegativeInt
    tips_count: NonNegativeFloat
    earned_fp_amount: NonNegativeFloat = field(name="EarnedFPAmount")

    is_paid: bool
    can_comment: bool
    description: str

    author_info: AuthorInfoField


async def is_stored(item: CollectionArticleInfo) -> bool:
    result = await COLLECTION.find_one({"slug": item.slug})
    if result:
        return True

    return False


async def init_db() -> None:
    await COLLECTION.create_indexes([IndexModel(("date", "slug"), unique=True)])


async def process_item(
    item: CollectionArticleInfo, /, *, current_date: date
) -> Optional[LPRecommendedArticleRecord]:
    logger = get_run_logger()

    if await is_stored(item):
        logger.warning(f"已保存过该文章记录，跳过 slug={item.slug}")
        return None

    return LPRecommendedArticleRecord(
        date=current_date,
        id=item.id,
        slug=item.slug,
        title=item.title,
        published_at=item.published_at,
        views_count=item.views_count,
        likes_count=item.likes_count,
        comments_count=item.comments_count,
        tips_count=item.tips_count,
        earned_fp_amount=item.earned_fp_amount,
        is_paid=item.is_paid,
        can_comment=item.can_comment,
        description=item.description,
        author_info=AuthorInfoField(
            id=item.author_info.id,
            slug=item.author_info.slug,
            name=item.author_info.name,
        ),
    ).validate()


@flow(
    **generate_flow_config(
        name="采集 LP 推荐文章记录",
    )
)
async def flow_func() -> State:
    await init_db()

    logger = get_run_logger()

    current_date = datetime.now().date()

    data: List[LPRecommendedArticleRecord] = []
    itered_items_count = 0
    async for item in LP_RECOMMENDED_COLLECTION.iter_articles():
        processed_item = await process_item(item, current_date=current_date)
        if processed_item:
            data.append(processed_item)

        itered_items_count += 1
        if itered_items_count == 100:
            break

    if data:
        await COLLECTION.insert_many(x.to_dict() for x in data)
    else:
        logger.info("无数据，不执行保存操作")

    return Completed(message=f"data_count={len(data)}")


deployment = flow_func.to_deployment(
    **generate_deployment_config(
        name="采集 LP 推荐文章记录",
        cron="0 1 * * *",
    )
)
