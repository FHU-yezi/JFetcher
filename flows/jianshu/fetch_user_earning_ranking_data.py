from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import date, datetime, timedelta

from jkit.config import CONFIG as JKIT_CONFIG
from jkit.ranking.user_earning import RecordData, UserEarningRanking
from jkit.user import InfoData
from prefect import flow, get_run_logger, task
from sshared.retry import retry

from models.jianshu.user import User
from models.jianshu.user_earning_ranking_record import (
    UserEarningRankingRecord,
    UserEarningRankingRecordType,
)
from utils.config import CONFIG
from utils.exceptions import DataExistsError
from utils.prefect_helper import get_flow_run_name, get_task_run_name
from utils.retry import NETWORK_REQUEST_RETRY_PARAMS

TOTAL_DATA_COUNT = 100

JKIT_CONFIG.data_validation.enabled = False
if CONFIG.jianshu_endpoint:
    JKIT_CONFIG.datasources.jianshu.endpoint = CONFIG.jianshu_endpoint


@retry(**NETWORK_REQUEST_RETRY_PARAMS)
async def get_user_info(
    item: RecordData,
) -> InfoData:
    return await item.to_user_obj().info


@task(task_run_name=get_task_run_name)
async def pre_check(*, date: date, type: UserEarningRankingRecordType) -> int:
    logger = get_run_logger()

    current_data_count = await UserEarningRankingRecord.count_by_date_and_type(
        date=date, type=type
    )
    if current_data_count == TOTAL_DATA_COUNT:
        raise DataExistsError(f"该日期的数据已存在 {date=}")
    if 0 < current_data_count < TOTAL_DATA_COUNT:
        logger.warning("正在进行断点续采 current_data_count=%s", current_data_count)

    return current_data_count + 1


@task(task_run_name=get_task_run_name)
async def iter_user_earning_ranking(
    date: date, type: UserEarningRankingRecordType
) -> AsyncGenerator[RecordData]:
    async for item in UserEarningRanking(date).iter_records(type=type):
        yield item


async def save_user_data(item: RecordData, /) -> None:
    logger = get_run_logger()

    if not item.slug or not item.name or not item.avatar_url:
        logger.warning("用户数据不可用，跳过采集 ranking=%s", item.ranking)

    user_info: InfoData = await get_user_info(item)

    user = await User.get_by_slug(user_info.slug)
    if not user:
        await User.create(
            slug=user_info.slug,
            id=user_info.id,
            name=user_info.name,
            avatar_url=user_info.avatar_url,
            membership_type=user_info.membership_info.type,
            membership_expire_time=user_info.membership_info.expire_time,
        )
    else:
        await User.update_by_slug(
            slug=user_info.slug,
            name=user_info.name,
            avatar_url=user_info.avatar_url,
            membership_type=user_info.membership_info.type,
            membership_expire_time=user_info.membership_info.expire_time,
        )


async def save_user_earning_ranking_record_data(
    item: RecordData, /, *, date: date, type: UserEarningRankingRecordType
) -> None:
    await UserEarningRankingRecord.create(
        date=date,
        type=type,
        ranking=item.ranking,
        slug=item.slug,
        total_earning=item.total_fp_amount,
        creating_earning=item.fp_by_creating_anount,
        voting_earning=item.fp_by_voting_amount,
    )


@flow(
    name="采集简书用户收益排行榜数据",
    flow_run_name=get_flow_run_name,
    retries=2,
    retry_delay_seconds=300,
    timeout_seconds=300,
)
async def jianshu_fetch_user_earning_ranking_data(
    type: UserEarningRankingRecordType, date: date | None = None
) -> None:
    logger = get_run_logger()

    if not date:
        date = datetime.now().date() - timedelta(days=1)

    start_ranking = await pre_check(date=date, type=type)

    async for item in iter_user_earning_ranking(date=date, type=type):
        # 断点续采
        if item.ranking < start_ranking:
            continue

        try:
            await save_user_data(item)
        except Exception:
            logger.exception("保存用户数据时发生未知异常 ranking=%s", item.ranking)

        try:
            await save_user_earning_ranking_record_data(item, date=date, type=type)
        except Exception:
            logger.exception(
                "保存用户收益排行榜数据时发生未知异常 ranking=%s", item.ranking
            )
