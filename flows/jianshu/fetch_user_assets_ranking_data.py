from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import datetime

from httpcore import NetworkError, TimeoutException
from httpx import HTTPError
from jkit.config import CONFIG as JKIT_CONFIG
from jkit.exceptions import APIUnsupportedError, ResourceUnavailableError
from jkit.ranking.assets import AssetsRanking, AssetsRankingRecord
from prefect import flow, get_run_logger, task
from prefect.states import Completed, Failed, State
from sshared.retry import retry

from models.jianshu.user import User
from models.jianshu.user_assets_ranking_record import (
    UserAssetsRankingRecord as DbUserAssetsRankingRecord,
)
from utils.config import CONFIG
from utils.prefect_helper import get_flow_run_name, get_task_run_name

JKIT_CONFIG.data_validation.enabled = False
if CONFIG.jianshu_endpoint:
    JKIT_CONFIG.endpoints.jianshu = CONFIG.jianshu_endpoint


@retry(
    retries=3,
    base_delay=5,
    exceptions=(HTTPError, NetworkError, TimeoutException),
)
async def get_user_fp_ftn_assets(
    item: AssetsRankingRecord,
) -> tuple[float, float, float]:
    user = item.user_info.to_user_obj()

    fp_amount = await user.fp_amount
    assets_amount = await user.assets_amount
    ftn_amount = round(assets_amount - fp_amount, 3)
    return (fp_amount, ftn_amount, assets_amount)


@task(task_run_name=get_task_run_name)
async def iter_user_assets_ranking(
    total_count: int,
) -> AsyncGenerator[AssetsRankingRecord]:
    current_count = 0
    async for item in AssetsRanking():
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
async def jianshu_fetch_user_assets_ranking_data(total_count: int = 1000) -> State:
    logger = get_run_logger()

    date = datetime.now().date()

    if await DbUserAssetsRankingRecord.is_records_exist(date):
        logger.error("该日期的数据已存在 date=%s", date)
        return Failed()

    data: list[DbUserAssetsRankingRecord] = []
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

        await User.upsert(
            slug=item.user_info.slug,
            id=item.user_info.id,
            name=item.user_info.name,
            avatar_url=item.user_info.avatar_url,
        )

        try:
            fp_amount, ftn_amount, assets_amount = await get_user_fp_ftn_assets(item)
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
        except APIUnsupportedError:  # API 限制
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
                    fp=fp_amount,
                    ftn=ftn_amount,
                    assets=assets_amount,
                )
            )

    await save_data_to_db(data)

    return Completed()
