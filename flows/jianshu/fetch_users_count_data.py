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


# TODO: 移除多余注释
@flow(
    name="采集简书用户数量数据",
    flow_run_name=get_flow_run_name,
    retries=2,
    retry_delay_seconds=300,
    timeout_seconds=300,
)
async def jianshu_fetch_users_count_data(
    default_start_ranking: int = 18265000, initial_step: int = 2000, max_tries: int = 20
) -> None:
    logger = get_run_logger()

    date = datetime.now().date() - timedelta(days=1)

    # TODO: 从数据库获取昨日数据以加速查找收敛
    current_start_ranking = default_start_ranking
    current_step = initial_step
    logger.debug("开始尝试获取用户数量 start_ranking=%s", current_start_ranking)

    tries_count = 0
    while True:
        users_list: list[RecordData] = await get_users_list(
            start_ranking=current_start_ranking
        )
        logger.debug(
            "已获取用户列表 tries_count=%s min_ranking=%s max_ranking=%s",
            tries_count,
            users_list[0].ranking if users_list else None,
            users_list[-1].ranking if users_list else None,
        )

        # 已获取到用户列表末尾
        if 0 < len(users_list) < USERS_LIST_LENGTH:
            logger.info("已完成用户数量获取 users_count=%s", users_list[-1].ranking)
            break

        # 获取到用户列表中段，向后跳转
        if len(users_list) == USERS_LIST_LENGTH:
            current_start_ranking += current_step
        # 已超出用户列表范围，向前跳转
        if len(users_list) == 0:
            current_step //= 2
            current_start_ranking -= current_step
        logger.debug(
            "已调整获取参数 start_ranking=%s step=%s",
            current_start_ranking,
            current_step,
        )

        tries_count += 1
        if tries_count == max_tries:
            raise BinarySearchMaxTriesReachedError(f"已达到最大尝试次数 {max_tries=}")

    # TODO: 拆分成单独任务
    await UsersCountRecord.create(
        date=date,
        total_users_count=users_list[-1].ranking,
    )
