from typing import Generator

from JianshuResearchTools.convert import UserSlugToUserUrl
from JianshuResearchTools.exceptions import APIError, ResourceError
from JianshuResearchTools.objects import User, set_cache_status
from JianshuResearchTools.rank import GetAssetsRankData
from utils.log import run_logger
from utils.register import task_func
from utils.saver import Saver
from utils.time_helper import (
    get_now_without_mileseconds,
    get_today_in_datetime_obj,
)

set_cache_status(False)


def data_iterator(total_count: int) -> Generator:
    now = 1
    while True:
        data_part = GetAssetsRankData(now)
        for item in data_part:
            yield item
            now += 1
            if now > total_count:
                return


def data_processor(saver: Saver) -> None:
    for item in data_iterator(1000):
        data = {
            "date": get_today_in_datetime_obj(),
            "ranking": item["ranking"],
            "user": {
                "id": None,
                "url": None,
                "name": None,
            },
            "assets": {
                "FP": None,
                "FTN": None,
                # JRT 写错了
                "total": item["FP"],
            },
        }
        if not item["uid"]:  # 用户账号状态异常，相关信息无法获取
            run_logger.warning(
                "FETCHER", f"排名为 {item['ranking']} " "的用户账号状态异常，部分数据无法采集，已自动跳过"
            )
        else:
            data["user"]["id"] = item["uid"]
            data["user"]["url"] = UserSlugToUserUrl(item["uslug"])
            data["user"]["name"] = item["name"]

            try:
                user = User.from_slug(item["uslug"])
                data["assets"]["FP"] = user.FP_count
                data["assets"]["FTN"] = round(
                    data["assets"]["total"] - data["assets"]["FP"], 3
                )
            except (ResourceError, APIError):
                run_logger.warning(
                    "FETCHER", f"无法获取 id 为 {item['uid']} 的用户的简书贝和简书贝信息，已自动跳过"
                )

        saver.add_data(data)

    saver.final_save()


@task_func(
    task_name="简书资产排行榜",
    cron="0 0 12 1/1 * *",
    db_name="assets_rank",
    data_bulk_size=100,
)
def main(saver: Saver):
    start_time = get_now_without_mileseconds()

    data_processor(saver)

    stop_time = get_now_without_mileseconds()
    cost_time = (stop_time - start_time).total_seconds()

    return (True, saver.get_data_count(), cost_time, "")
