from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import date, datetime, timedelta

from jkit.config import CONFIG as JKIT_CONFIG
from jkit.ranking.user_earning import RecordData, UserEarningRanking
from jkit.user import InfoData
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

    current_data_count = await UserEarningRankingRecord.count_by_date(date)
    if current_data_count == TOTAL_DATA_COUNT:
        logger.error("该日期的数据已存在 date=%s", date)
        return Failed()
    if 0 < current_data_count < TOTAL_DATA_COUNT:
        # TODO: 实现断点续采
        raise NotImplementedError

    data: list[UserEarningRankingRecord] = []
    async for item in iter_user_earning_ranking(date=date, type=type):
        user_info: InfoData = await get_user_info(item)

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
