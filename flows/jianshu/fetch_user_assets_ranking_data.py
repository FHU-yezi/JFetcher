from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import date, datetime

from jkit.config import CONFIG as JKIT_CONFIG
from jkit.exceptions import (
    ResourceUnavailableError,
)
from jkit.ranking.user_assets import RecordData, UserAssetsRanking
from jkit.user import AssetsInfoData, InfoData
from prefect import flow, get_run_logger, task
from sshared.retry import retry

from models.jianshu.user import User
from models.jianshu.user_assets_ranking_record import (
    UserAssetsRankingRecord as DbUserAssetsRankingRecord,
)
from utils.config import CONFIG
from utils.prefect_helper import get_flow_run_name, get_task_run_name
from utils.retry import NETWORK_REQUEST_RETRY_PARAMS

JKIT_CONFIG.data_validation.enabled = False
if CONFIG.jianshu_endpoint:
    JKIT_CONFIG.datasources.jianshu.endpoint = CONFIG.jianshu_endpoint


@retry(**NETWORK_REQUEST_RETRY_PARAMS)
async def get_user_info(
    item: RecordData,
) -> InfoData:
    user = item.user_info.to_user_obj()

    return await user.info


@retry(**NETWORK_REQUEST_RETRY_PARAMS)
async def get_user_assets_info(
    item: RecordData,
) -> AssetsInfoData:
    user = item.user_info.to_user_obj()

    return await user.assets_info


@task(task_run_name=get_task_run_name)
async def pre_check(*, date: date, total_count: int) -> int:
    logger = get_run_logger()

    current_data_count = await DbUserAssetsRankingRecord.count_by_date(date)
    if current_data_count >= total_count:
        logger.error("该日期的数据已存在 date=%s", date)
    if 0 < current_data_count < total_count:
        logger.warning("正在进行断点续采 current_data_count=%s", current_data_count)

    return current_data_count + 1


@task(task_run_name=get_task_run_name)
async def iter_user_assets_ranking(
    *,
    start_ranking: int,
    total_count: int,
) -> AsyncGenerator[RecordData]:
    async for item in UserAssetsRanking(start_ranking=start_ranking).iter_records():
        yield item

        if item.ranking == total_count:
            return


async def save_user_data(item: RecordData, /) -> None:
    logger = get_run_logger()

    if (
        not item.user_info.id
        or not item.user_info.slug
        or not item.user_info.name
        or not item.user_info.avatar_url
    ):
        logger.info("用户信息不可用，跳过用户数据采集 raning=%s", item.ranking)
        return

    user = await User.get_by_slug(item.user_info.slug)

    try:
        user_info: InfoData = await get_user_info(item)
    except ResourceUnavailableError:
        # 用户不存在或已注销 / 被封禁，将其 status 设置为 INACCESSIBLE
        # 如果用户记录尚不存在，先创建
        if not user:
            await User.create(
                slug=item.user_info.slug,
                id=item.user_info.id,
                name=item.user_info.name,
                avatar_url=item.user_info.avatar_url,
                membership_type="NONE",
                membership_expire_time=None,
            )
        logger.info(
            "用户不存在或已注销 / 被封禁，status 已设为 INACCESSIBLE slug=%s",
            item.user_info.slug,
        )
    else:
        if not user:
            await User.create(
                slug=item.user_info.slug,
                id=item.user_info.id,
                name=item.user_info.name,
                avatar_url=item.user_info.avatar_url,
                membership_type=user_info.membership_info.type,
                membership_expire_time=user_info.membership_info.expire_time,
            )
        else:
            await User.update_by_slug(
                slug=item.user_info.slug,
                name=item.user_info.name,
                avatar_url=item.user_info.avatar_url,
                membership_type=user_info.membership_info.type,
                membership_expire_time=user_info.membership_info.expire_time,
            )


async def save_user_assets_ranking_record_data(
    item: RecordData, /, *, date: date
) -> None:
    logger = get_run_logger()

    try:
        assets_info: AssetsInfoData = await get_user_assets_info(item)
    except ResourceUnavailableError:
        assets_info = None  # type: ignore
    else:
        if assets_info.ftn_amount is None and assets_info.assets_amount is None:
            logger.warning(
                "受 API 限制，无法采集简书贝和总资产数据 ranking=%s slug=%s",
                item.ranking,
                item.user_info.slug,
            )

    await DbUserAssetsRankingRecord.create(
        date=date,
        ranking=item.ranking,
        slug=item.user_info.slug,
        fp=assets_info.fp_amount if assets_info else None,
        ftn=assets_info.ftn_amount if assets_info else None,
        # 后备数据（资产排行榜，非实时）
        assets=assets_info.assets_amount if assets_info else item.assets_amount,
    )


@flow(
    name="采集简书用户资产排行榜数据",
    flow_run_name=get_flow_run_name,
    retries=2,
    retry_delay_seconds=600,
    timeout_seconds=3600,
)
async def jianshu_fetch_user_assets_ranking_data(total_count: int = 3000) -> None:
    logger = get_run_logger()

    date = datetime.now().date()

    start_ranking = await pre_check(date=date, total_count=total_count)

    async for item in iter_user_assets_ranking(
        # 断点续采
        start_ranking=start_ranking,
        total_count=total_count,
    ):
        try:
            await save_user_data(item)
        except Exception:
            logger.exception("保存用户数据时发生未知异常 ranking=%s", item.ranking)

        try:
            await save_user_assets_ranking_record_data(item, date=date)
        except Exception:
            logger.exception(
                "保存用户收益排行榜数据时发生未知异常 ranking=%s", item.ranking
            )
