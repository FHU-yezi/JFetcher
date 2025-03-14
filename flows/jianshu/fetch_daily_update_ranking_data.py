from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import date, datetime

from jkit.config import CONFIG as JKIT_CONFIG
from jkit.ranking.daily_update import DailyUpdateRanking, RecordData
from jkit.user import InfoData as UserInfoData
from prefect import flow, get_run_logger, task
from sshared.retry import retry

from models.jianshu.daily_update_ranking_record import (
    DailyUpdateRankingRecord as DbDailyUpdateRankingRecord,
)
from models.jianshu.user import User
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
) -> UserInfoData:
    user = item.user_info.to_user_obj()

    return await user.info


@task(task_run_name=get_task_run_name)
async def pre_check(*, date: date) -> int:
    logger = get_run_logger()

    current_data_count = await DbDailyUpdateRankingRecord.count_by_date(date)
    if current_data_count == TOTAL_DATA_COUNT:
        raise DataExistsError(f"该日期的数据已存在 {date=}")
    if 0 < current_data_count < TOTAL_DATA_COUNT:
        logger.warning("正在进行断点续采 current_data_count=%s", current_data_count)

    return current_data_count + 1


@task(task_run_name=get_task_run_name)
async def iter_daily_update_ranking() -> AsyncGenerator[RecordData]:
    async for item in DailyUpdateRanking().iter_records():
        yield item


async def save_user_data(item: RecordData, /) -> None:
    user_info: UserInfoData = await get_user_info(item)

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


async def save_daily_update_ranking_record_data(
    item: RecordData, /, *, date: date
) -> None:
    await DbDailyUpdateRankingRecord.create(
        date=date,
        ranking=item.ranking,
        slug=item.user_info.slug,
        days=item.days,
    )


@flow(
    name="采集简书日更排行榜数据",
    flow_run_name=get_flow_run_name,
    retries=2,
    retry_delay_seconds=300,
    timeout_seconds=300,
)
async def jianshu_fetch_daily_update_ranking_data() -> None:
    logger = get_run_logger()

    date = datetime.now().date()

    start_ranking = await pre_check(date=date)

    async for item in iter_daily_update_ranking():
        # 断点续采
        if item.ranking < start_ranking:
            continue

        try:
            await save_user_data(item)
        except Exception:
            logger.exception("保存用户数据时发生未知异常 ranking=%s", item.ranking)

        try:
            await save_daily_update_ranking_record_data(item, date=date)
        except Exception:
            logger.exception(
                "保存日更排行榜数据时发生未知异常 ranking=%s", item.ranking
            )
