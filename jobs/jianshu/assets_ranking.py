from datetime import datetime
from typing import List, Optional, Tuple

from jkit.config import CONFIG
from jkit.exceptions import ResourceUnavailableError
from jkit.ranking.assets import AssetsRanking, AssetsRankingRecord
from jkit.user import User
from prefect import flow
from prefect.states import Completed, State
from sshared.retry.asyncio import retry
from sshared.time import get_today_as_datetime

from models.jianshu.assets_ranking_record import (
    AmountField,
    AssetsRankingRecordDocument,
)
from models.jianshu.user import UserDocument
from utils.config_generators import (
    generate_deployment_config,
    generate_flow_config,
)
from utils.log import logger


@retry(attempts=5, delay=10)
async def get_fp_ftn_amount(
    item: AssetsRankingRecord, /
) -> Tuple[Optional[float], Optional[float]]:
    if not item.user_info.slug:
        logger.warn(
            f"用户已注销或被封禁，跳过采集简书钻与简书贝信息 ranking={item.ranking}"
        )
        return None, None

    try:
        # TODO: 临时解决简书系统问题数据负数导致的报错
        CONFIG.data_validation.enabled = False
        # 此处使用用户 Slug 初始化用户对象，以对其进行可用性检查
        user_obj = User.from_slug(item.user_info.slug)
        fp_amount = await user_obj.fp_amount
        ftn_amount = abs(round(item.assets_amount - fp_amount, 3))
        CONFIG.data_validation.enabled = True

        return fp_amount, ftn_amount
    except ResourceUnavailableError:
        logger.warn(
            f"用户已注销或被封禁，跳过采集简书钻与简书贝信息 ranking={item.ranking}"
        )
        return None, None


async def process_item(
    item: AssetsRankingRecord, /, *, target_date: datetime
) -> AssetsRankingRecordDocument:
    fp_amount, ftn_amount = await get_fp_ftn_amount(item)

    if item.user_info.slug:
        await UserDocument.insert_or_update_one(
            slug=item.user_info.slug,
            id=item.user_info.id,
            name=item.user_info.name,
            avatar_url=item.user_info.avatar_url,
        )

    return AssetsRankingRecordDocument(
        date=target_date,
        ranking=item.ranking,
        amount=AmountField(
            fp=fp_amount,
            ftn=ftn_amount,
            assets=item.assets_amount,
        ),
        user_slug=item.user_info.slug,
    ).validate()


@flow(
    **generate_flow_config(
        name="采集资产排行榜记录",
    )
)
async def flow_func() -> State:
    target_date = get_today_as_datetime()

    data: List[AssetsRankingRecordDocument] = []
    async for item in AssetsRanking():
        processed_item = await process_item(item, target_date=target_date)
        data.append(processed_item)

        if len(data) == 1000:
            break

    await AssetsRankingRecordDocument.insert_many(data)

    return Completed(message=f"target_date={target_date}, data_count={len(data)}")


deployment = flow_func.to_deployment(
    **generate_deployment_config(
        name="采集资产排行榜记录",
        cron="0 1 * * *",
    )
)