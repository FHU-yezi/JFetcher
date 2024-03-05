from datetime import date, datetime
from typing import List, Optional

from jkit.collection import Collection, CollectionArticleInfo
from prefect import flow, get_run_logger
from prefect.states import Completed, State

from models.jianshu_user import init_db as init_jianshu_user_db
from models.jianshu_user import insert_or_update_one
from models.lp_recommend_article_record import (
    LPRecommendedArticleRecord,
    insert_many,
    is_record_stored,
)
from models.lp_recommend_article_record import (
    init_db as init_lp_recommend_article_record_db,
)
from utils.config_generators import (
    generate_deployment_config,
    generate_flow_config,
)

# 理事会点赞汇总专题
LP_RECOMMENDED_COLLECTION = Collection.from_slug("f61832508891")


async def process_item(
    item: CollectionArticleInfo, /, *, current_date: date
) -> Optional[LPRecommendedArticleRecord]:
    logger = get_run_logger()

    if await is_record_stored(item.slug):
        logger.warning(f"已保存过该文章记录，跳过 slug={item.slug}")
        return None

    await insert_or_update_one(
        slug=item.author_info.slug,
        id=item.author_info.id,
        name=item.author_info.name,
        avatar_url=item.author_info.avatar_url,
    )

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
        author_slug=item.author_info.slug,
    ).validate()


@flow(
    **generate_flow_config(
        name="采集 LP 推荐文章记录",
    )
)
async def flow_func() -> State:
    await init_lp_recommend_article_record_db()
    await init_jianshu_user_db()

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
        await insert_many(data)
    else:
        logger.info("无数据，不执行保存操作")

    return Completed(message=f"data_count={len(data)}")


deployment = flow_func.to_deployment(
    **generate_deployment_config(
        name="采集 LP 推荐文章记录",
        cron="0 1 * * *",
    )
)
