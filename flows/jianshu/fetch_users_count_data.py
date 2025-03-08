from __future__ import annotations

from datetime import date, datetime, timedelta

from jkit.config import CONFIG as JKIT_CONFIG
from jkit.ranking.user_assets import RecordData, UserAssetsRanking
from prefect import flow, get_run_logger, task
from sshared.retry import retry

from models.jianshu.users_count_record import UsersCountRecord
from utils.config import CONFIG
from utils.exceptions import BinarySearchMaxTriesReachedError, DataExistsError
from utils.prefect_helper import get_flow_run_name, get_task_run_name
from utils.retry import NETWORK_REQUEST_RETRY_PARAMS

USERS_LIST_LENGTH = 20
DEFAULT_START_RANKING = 18270000

JKIT_CONFIG.data_validation.enabled = False
if CONFIG.jianshu_endpoint:
    JKIT_CONFIG.datasources.jianshu.endpoint = CONFIG.jianshu_endpoint


@retry(**NETWORK_REQUEST_RETRY_PARAMS)
async def get_users_list(start_ranking: int) -> list[RecordData]:
    result: list[RecordData] = []
    async for item in UserAssetsRanking(start_ranking=start_ranking).iter_records():
        result.append(item)

        if len(result) == USERS_LIST_LENGTH:
            return result

    return result


@task(task_run_name=get_task_run_name)
async def pre_check(date: date) -> None:
    if await UsersCountRecord.exists_by_date(date):
        raise DataExistsError(f"该日期的数据已存在 {date=}")


@task(task_run_name=get_task_run_name)
async def save_data_to_db(*, date: date, total_users_count: int) -> None:
    await UsersCountRecord.create(
        date=date,
        total_users_count=total_users_count,
    )


@flow(
    name="采集简书用户数量数据",
    flow_run_name=get_flow_run_name,
    retries=2,
    retry_delay_seconds=300,
    timeout_seconds=300,
)
async def jianshu_fetch_users_count_data(
    initial_step: int = 1000, max_tries: int = 20
) -> None:
    logger = get_run_logger()

    date = datetime.now().date() - timedelta(days=1)

    await pre_check(date=date)

    latest_data = await UsersCountRecord.get_by_date(date - timedelta(days=1))
    if not latest_data:
        start_ranking = DEFAULT_START_RANKING
        logger.warning(
            "获取数据库最新记录失败，将使用默认参数 start_ranking=%s",
            start_ranking,
        )
    else:
        start_ranking = latest_data.total_users_count
        logger.warning(
            "正在使用数据库历史参数加速查找收敛 start_ranking=%s",
            start_ranking,
        )

    step = initial_step
    tries = 1
    while True:
        logger.info(
            "正在获取资产排行榜数据 tries=%s start_ranking=%s step=%s",
            tries,
            start_ranking,
            step,
        )
        users_list: list[RecordData] = await get_users_list(start_ranking=start_ranking)

        # 已获取到用户列表末尾
        if 0 < len(users_list) < USERS_LIST_LENGTH:
            break

        # 获取到用户列表中段，向后跳转
        if len(users_list) == USERS_LIST_LENGTH:
            start_ranking += step
        # 已超出用户列表范围，向前跳转
        if len(users_list) == 0:
            step //= 2
            start_ranking -= step

        if tries == max_tries:
            raise BinarySearchMaxTriesReachedError(f"已达到最大尝试次数 {max_tries=}")

        tries += 1

    await save_data_to_db(
        date=date,
        total_users_count=users_list[-1].ranking,
    )
