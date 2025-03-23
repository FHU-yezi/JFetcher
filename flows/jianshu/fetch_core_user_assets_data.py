from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import datetime, timedelta

from jkit.config import CONFIG as JKIT_CONFIG
from jkit.exceptions import (
    ResourceUnavailableError,
)
from jkit.user import AssetsInfoData, InfoData, User
from prefect import flow, get_run_logger, task
from sshared.retry import retry

from models.jianshu.core_user_assets_record import CoreUserAssetsRecord
from models.jianshu.user import User as DbUser
from utils.config import CONFIG
from utils.prefect_helper import get_flow_run_name, get_task_run_name
from utils.retry import NETWORK_REQUEST_RETRY_PARAMS

JKIT_CONFIG.data_validation.enabled = False
if CONFIG.jianshu_endpoint:
    JKIT_CONFIG.datasources.jianshu.endpoint = CONFIG.jianshu_endpoint


def get_fetch_time() -> datetime:
    time = datetime.now().replace(second=0, microsecond=0)

    # 12:13 -> 12:00
    if time.minute < 30:
        return time.replace(minute=0)
    # 12:49 -> 13:00
    else:  # noqa: RET505
        return time.replace(minute=0) + timedelta(hours=1)


@retry(**NETWORK_REQUEST_RETRY_PARAMS)
async def get_user_info(item: User) -> InfoData:
    return await item.info


@retry(**NETWORK_REQUEST_RETRY_PARAMS)
async def get_user_assets_info(item: User) -> AssetsInfoData:
    return await item.assets_info


@task(task_run_name=get_task_run_name)
async def iter_core_users() -> AsyncGenerator[User]:
    for slug in CONFIG.core_user_slugs:
        yield User.from_slug(slug)


async def save_user_data(item: User, /) -> None:
    logger = get_run_logger()

    # 以高频率采集用户数据无意义
    # 故如果用户数据已存在，跳过采集
    user = await DbUser.get_by_slug(item.slug)
    if user:
        return

    try:
        user_info: InfoData = await get_user_info(item)
    except ResourceUnavailableError:
        # 用户已注销 / 被封禁，创建用户记录无意义
        logger.info("用户已注销 / 被封禁，跳过创建用户记录 slug=%s", item.slug)
    else:
        await DbUser.create(
            slug=user_info.slug,
            id=user_info.id,
            name=user_info.name,
            avatar_url=user_info.avatar_url,
            membership_type=user_info.membership_info.type,
            membership_expire_time=user_info.membership_info.expire_time,
        )


async def save_core_user_assets_record_data(item: User, /, *, time: datetime) -> None:
    logger = get_run_logger()

    try:
        assets_info: AssetsInfoData = await get_user_assets_info(item)
    except ResourceUnavailableError:
        logger.info("用户已注销 / 被封禁，跳过采集资产数据 slug=%s", item.slug)
    else:
        await CoreUserAssetsRecord.create(
            time=time,
            slug=item.slug,
            fp=assets_info.fp_amount,
            ftn=assets_info.ftn_amount,
            assets=assets_info.assets_amount,
        )


@flow(
    name="采集简书核心用户资产数据",
    flow_run_name=get_flow_run_name,
    retries=2,
    retry_delay_seconds=600,
    timeout_seconds=3600,
)
async def jianshu_fetch_core_user_assets_data() -> None:
    logger = get_run_logger()

    time = get_fetch_time()

    async for item in iter_core_users():
        try:
            await save_user_data(item)
        except Exception:
            logger.exception("保存用户数据时发生未知异常 slug=%s", item.slug)

        try:
            await save_core_user_assets_record_data(item, time=time)
        except Exception:
            logger.exception("保存核心用户资产数据时发生未知异常 slug=%s", item.slug)
