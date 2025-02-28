from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import date, datetime, timedelta

from jkit.config import CONFIG as JKIT_CONFIG
from jkit.ranking.user_earning import RecordData, UserEarningRanking
from prefect import flow, get_run_logger, task
from prefect.client.schemas.objects import State
from prefect.states import Completed, Failed
from sshared.retry import retry

from models.jianshu.user import User
from models.jianshu.user_earning_ranking_record import (
    UserEarningRankingRecord,
    UserEarningRankingRecordType,
)
from utils.config import CONFIG
from utils.prefect_helper import get_flow_run_name, get_task_run_name
from utils.retry import get_network_request_retry_params

JKIT_CONFIG.data_validation.enabled = False
if CONFIG.jianshu_endpoint:
    JKIT_CONFIG.datasources.jianshu.endpoint = CONFIG.jianshu_endpoint


@retry(**get_network_request_retry_params())
async def get_user_id(
    item: RecordData,
) -> int:
    return await item.to_user_obj().id


@task(task_run_name=get_task_run_name)
async def iter_user_earning_ranking(
    date: date, type: UserEarningRankingRecordType
) -> AsyncGenerator[RecordData]:
    async for item in UserEarningRanking(date).iter_records(type=type):
        yield item


@task(task_run_name=get_task_run_name)
async def save_data_to_db(data: list[UserEarningRankingRecord]) -> None:
    await UserEarningRankingRecord.insert_many(data)


@flow(
    name="采集简书用户收益排行榜数据",
    flow_run_name=get_flow_run_name,
    retries=2,
    retry_delay_seconds=300,
    timeout_seconds=300,
)
async def jianshu_fetch_user_earning_ranking_data(
    type: UserEarningRankingRecordType, date: date | None = None
) -> State:
    logger = get_run_logger()

    if not date:
        date = datetime.now().date() - timedelta(days=1)

    if await UserEarningRankingRecord.is_records_exist(date=date, type=type):
        logger.error("该日期的数据已存在 date=%s", date)
        return Failed()

    data: list[UserEarningRankingRecord] = []
    async for item in iter_user_earning_ranking(date=date, type=type):
        user_id = await get_user_id(item)

        await User.upsert(
            slug=item.slug,
            id=user_id,
            name=item.name,
            avatar_url=item.avatar_url,
        )

        data.append(
            UserEarningRankingRecord(
                date=date,
                type=type,
                ranking=item.ranking,
                slug=item.slug,
                total_earning=item.total_fp_amount,
                creating_earning=item.fp_by_creating_anount,
                voting_earning=item.fp_by_voting_amount,
            )
        )

    await save_data_to_db(data)

    return Completed()
