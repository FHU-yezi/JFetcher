from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import date, datetime

from jkit.config import CONFIG as JKIT_CONFIG
from jkit.exceptions import (
    RatelimitError,
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
async def pre_check(date: date, total_count: int) -> int:
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
    current_count = 0
    async for item in UserAssetsRanking(start_ranking=start_ranking).iter_records():
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
async def jianshu_fetch_user_assets_ranking_data(total_count: int = 3000) -> None:
    logger = get_run_logger()

    date = datetime.now().date()

    start_ranking = await pre_check(date=date, total_count=total_count)

    data: list[DbUserAssetsRankingRecord] = []
    try:
        async for item in iter_user_assets_ranking(
            start_ranking=start_ranking, total_count=total_count
        ):
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

            try:
                user_info: InfoData = await get_user_info(item)
            except ResourceUnavailableError:
                # 用户不存在或已注销 / 被封禁
                # 如果数据库中有该用户的记录，将其 status 设置为 INACCESSIBLE
                # 否则，不做任何事，因为采集不存在的用户数据没有意义
                user = await User.get_by_slug(item.user_info.slug)
                if user:
                    await User.update_status_by_slug(
                        slug=item.user_info.slug, status="INACCESSIBLE"
                    )
                    logger.info(
                        "用户不存在或已注销 / 被封禁，status 已设为 "
                        "INACCESSIBLE slug=%s",
                        item.user_info.slug,
                    )

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
                user = await User.get_by_slug(item.user_info.slug)
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

            # 此时不可能再次出现 ResourceUnavaliableError
            # 因此不做错误处理
            assets_info: AssetsInfoData = await get_user_assets_info(item)
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
    except RatelimitError:
        await save_data_to_db(data)
        raise RatelimitError("触发简书限流，已保存部分数据") from None
