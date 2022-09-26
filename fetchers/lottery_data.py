from datetime import datetime
from typing import Generator

from httpx import get as httpx_get
from JianshuResearchTools.convert import UserSlugToUserUrl
from utils.log import run_logger
from utils.register import task_func
from utils.saver import Saver
from utils.time_helper import get_now_without_mileseconds


def is_already_in_db(db, id: int) -> int:
    return db.count_documents({"_id": id}) != 0


def data_iterator() -> Generator:
    params = {
        "count": 500,
    }
    response = httpx_get(
        "https://www.jianshu.com/asimov/ad_rewards/winner_list",
        params=params,
        timeout=20,
    )
    try:
        response.raise_for_status()
    except Exception:
        run_logger.error("FETCHER", "无法获取大转盘抽奖数据")
        return

    data_part = response.json()
    for item in data_part:
        yield item
    return


def data_processor(saver: Saver) -> None:
    for item in data_iterator():
        # 特殊需求，需要获取底层数据库对象
        if is_already_in_db(saver._db, item["id"]):
            continue

        data = {
            "_id": item["id"],
            "time": datetime.fromtimestamp(item["created_at"]),
            "reward_name": item["name"],
            "user": {
                "id": item["user"]["id"],
                "url": UserSlugToUserUrl(item["user"]["slug"]),
                "name": item["user"]["nickname"],
            },
        }

        saver.add_data(data)

    saver.final_save()


@task_func(
    task_name="简书大转盘抽奖",
    cron="0 0 2,9,14,21 1/1 * *",
    db_name="lottery_data",
    data_bulk_size=100,
)
def main(saver: Saver):

    start_time = get_now_without_mileseconds()

    data_processor(saver)

    stop_time = get_now_without_mileseconds()
    cost_time = (stop_time - start_time).total_seconds()

    return (True, saver.get_data_count(), cost_time, "")
