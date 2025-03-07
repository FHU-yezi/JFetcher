from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import datetime

from jkit.config import CONFIG as JKIT_CONFIG
from jkit.ranking.daily_update import DailyUpdateRanking, RecordData
from jkit.user import InfoData as UserInfoData
from prefect import flow, get_run_logger, task
from prefect.client.schemas.objects import State
from prefect.states import Completed, Failed
from sshared.retry import retry

from models.jianshu.daily_update_ranking_record import (
    DailyUpdateRankingRecord as DbDailyUpdateRankingRecord,
)
from models.jianshu.user import User
from utils.config import CONFIG
from utils.prefect_helper import get_flow_run_name, get_task_run_name
from utils.retry import get_network_request_retry_params

TOTAL_DATA_COUNT = 100

JKIT_CONFIG.data_validation.enabled = False
if CONFIG.jianshu_endpoint:
    JKIT_CONFIG.datasources.jianshu.endpoint = CONFIG.jianshu_endpoint


@retry(**get_network_request_retry_params())
async def get_user_info(
    item: RecordData,
) -> UserInfoData:
    user = item.user_info.to_user_obj()

    return await user.info


@task(task_run_name=get_task_run_name)
async def iter_daily_update_ranking() -> AsyncGenerator[RecordData]:
    async for item in DailyUpdateRanking().iter_records():
        yield item


@task(task_run_name=get_task_run_name)
async def save_data_to_db(data: list[DbDailyUpdateRankingRecord]) -> None:
    await DbDailyUpdateRankingRecord.insert_many(data)


@flow(
    name="采集简书日更排行榜数据",
    flow_run_name=get_flow_run_name,
    retries=2,
    retry_delay_seconds=300,
    timeout_seconds=300,
)
async def jianshu_fetch_daily_update_ranking_data() -> State:
    logger = get_run_logger()

    date = datetime.now().date()

    current_data_count = await DbDailyUpdateRankingRecord.count_by_date(date)
    if current_data_count == TOTAL_DATA_COUNT:
        logger.error("该日期的数据已存在 date=%s", date)
        return Failed()
    if 0 < current_data_count < TOTAL_DATA_COUNT:
        # TODO: 实现断点续采
        raise NotImplementedError

    data: list[DbDailyUpdateRankingRecord] = []
    async for item in iter_daily_update_ranking():
        user_info: UserInfoData = await get_user_info(item)

        user = await User.get_by_slug(user_info.slug)
        if not user:
            await User.create(
                slug=user_info.slug,
                id=user_info.id,
                name=user_info.name,
                avatar_url=user_info.avatar_url,
            )
        else:
            await User.update_by_slug(
                slug=user_info.slug,
                name=user_info.name,
                avatar_url=user_info.avatar_url,
            )

        data.append(
            DbDailyUpdateRankingRecord(
                date=date,
                ranking=item.ranking,
                slug=item.user_info.slug,
                days=item.days,
            )
        )

    await save_data_to_db(data)

    return Completed()
