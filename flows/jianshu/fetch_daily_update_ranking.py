from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import datetime

from jkit.config import CONFIG
from jkit.ranking.daily_update import DailyUpdateRanking, DailyUpdateRankingRecord
from prefect import flow, get_run_logger, task
from prefect.states import Completed, Failed, State

from models.jianshu.daily_update_ranking_record import (
    DailyUpdateRankingRecord as DbDailyUpdateRankingRecord,
)
from models.jianshu.user import User as DbUser
from utils.prefect_helper import get_flow_run_name, get_task_run_name

CONFIG.data_validation.enabled = False
CONFIG.endpoints.jianshu = "https://main-jianshu-proxy-sqrodthfab.cn-beijing.fcapp.run"


@task(task_run_name=get_task_run_name)
async def iter_daily_update_ranking() -> AsyncGenerator[DailyUpdateRankingRecord]:
    async for item in DailyUpdateRanking():
        yield item


@task(task_run_name=get_task_run_name)
async def save_data_to_db(data: list[DbDailyUpdateRankingRecord]) -> None:
    await DbDailyUpdateRankingRecord.insert_many(data)


@flow(
    name="采集简书日更排行榜数据",
    flow_run_name=get_flow_run_name,
    retries=1,
    retry_delay_seconds=180,
    timeout_seconds=300,
)
async def jianshu_fetch_daily_update_ranking_data() -> State:
    logger = get_run_logger()

    date = datetime.now().date()

    if await DbDailyUpdateRankingRecord.is_records_exist(date):
        logger.warning("该日期的数据已存在 date=%s", date)
        return Failed()

    data: list[DbDailyUpdateRankingRecord] = []
    async for item in iter_daily_update_ranking():
        user = item.user_info.to_user_obj()
        user_info = await user.info

        await DbUser.upsert(
            slug=user.slug,
            id=user_info.id,
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
