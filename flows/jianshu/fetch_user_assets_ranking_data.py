from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import date, datetime

from jkit.config import CONFIG as JKIT_CONFIG
from jkit.exceptions import (
    ResourceUnavailableError,
)
from jkit.ranking.user_assets import RecordData, UserAssetsRanking
from jkit.user import AssetsInfoData
from prefect import flow, get_run_logger, task
from sshared.retry import retry

from models.jianshu.user import MEMBERSHIP_INFO_UNAVALIABLE, User
from models.jianshu.user_assets_ranking_record import (
    UserAssetsRankingRecord as DbUserAssetsRankingRecord,
)
from utils.config import CONFIG
from utils.prefect_helper import get_flow_run_name, get_task_run_name
from utils.retry import NETWORK_REQUEST_RETRY_PARAMS

if CONFIG.jianshu_endpoint:
    JKIT_CONFIG.datasources.jianshu.endpoint = CONFIG.jianshu_endpoint


@retry(**NETWORK_REQUEST_RETRY_PARAMS)
async def get_user_assets_info(
    item: RecordData,
) -> AssetsInfoData:
    user = item.user_info.to_user_obj()

    return await user.assets_info


@task(task_run_name=get_task_run_name)
async def pre_check(date: date, total_count: int) -> None:
    logger = get_run_logger()

    current_data_count = await DbUserAssetsRankingRecord.count_by_date(date)
    if current_data_count >= total_count:
        logger.error("该日期的数据已存在 date=%s", date)
    if 0 < current_data_count < total_count:
        logger.warning("正在进行断点续采 current_data_count=%s", current_data_count)


@task(task_run_name=get_task_run_name)
async def iter_user_assets_ranking(
    total_count: int,
) -> AsyncGenerator[RecordData]:
    current_count = 0
    async for item in UserAssetsRanking().iter_records():
        yield item
        current_count += 1

        if current_count == total_count:
            return


@task(task_run_name=get_task_run_name)
async def save_data_to_db(data: list[DbUserAssetsRankingRecord]) -> None:
    await DbUserAssetsRankingRecord.insert_many(data)


@flow(
    name="采集简书用户资产排行榜数据",
    flow_run_name=get_flow_run_name,
    retries=2,
    retry_delay_seconds=600,
    timeout_seconds=3600,
)
async def jianshu_fetch_user_assets_ranking_data(total_count: int = 1000) -> None:
    logger = get_run_logger()

    date = datetime.now().date()

    await pre_check(date=date, total_count=total_count)

    data: list[DbUserAssetsRankingRecord] = []
    # TODO: 实现断点续采
    async for item in iter_user_assets_ranking(total_count):
        if (
            item.user_info.slug is None
            or item.user_info.id is None
            or item.user_info.name is None
            or item.user_info.avatar_url is None
        ):  # 用户数据为空
            logger.warning(
                "用户数据为空，跳过资产数据采集 ranking=%s",
                item.ranking,
            )
            continue

        user = await User.get_by_slug(item.user_info.slug)
        if not user:
            await User.create(
                slug=item.user_info.slug,
                id=item.user_info.id,
                name=item.user_info.name,
                avatar_url=item.user_info.avatar_url,
                # 数据不可用，默认无会员
                # TODO: 优化采集策略，尽可能采集会员信息
                membership_type="NONE",
                membership_expire_time=None,
            )
        else:
            await User.update_by_slug(
                slug=item.user_info.slug,
                name=item.user_info.name,
                avatar_url=item.user_info.avatar_url,
                # TODO: 优化采集策略，尽可能采集会员信息
                membership_type=MEMBERSHIP_INFO_UNAVALIABLE,
                membership_expire_time=MEMBERSHIP_INFO_UNAVALIABLE,
            )

        try:
            assets_info: AssetsInfoData = await get_user_assets_info(item)
        except ResourceUnavailableError:  # 用户状态异常
            logger.warning(
                "用户状态异常，跳过资产数据采集 ranking=%s slug=%s",
                item.ranking,
                item.user_info.slug,
            )
            data.append(
                DbUserAssetsRankingRecord(
                    date=date,
                    ranking=item.ranking,
                    slug=item.user_info.slug,
                    fp=None,
                    ftn=None,
                    # 后备数据（资产排行榜，非实时）
                    assets=item.assets_amount,
                )
            )
        else:
            if assets_info.ftn_amount is None and assets_info.assets_amount is None:
                logger.warning(
                    "受 API 限制，无法采集简书贝和总资产数据 ranking=%s slug=%s",
                    item.ranking,
                    item.user_info.slug,
                )
                # 此时 fp_amount 为实时数据，assets_amount 为非实时数据
                # 为防止数据异常，不对 fp_amount 进行存储
                data.append(
                    DbUserAssetsRankingRecord(
                        date=date,
                        ranking=item.ranking,
                        slug=item.user_info.slug,
                        fp=None,
                        ftn=None,
                        # 后备数据（资产排行榜，非实时）
                        assets=item.assets_amount,
                    )
                )
            else:  # 正常
                data.append(
                    DbUserAssetsRankingRecord(
                        date=date,
                        ranking=item.ranking,
                        slug=item.user_info.slug,
                        fp=assets_info.fp_amount,
                        ftn=assets_info.ftn_amount,
                        assets=assets_info.assets_amount,
                    )
                )

    await save_data_to_db(data)
