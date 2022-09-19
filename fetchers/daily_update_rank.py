from typing import Generator

from JianshuResearchTools.convert import UserSlugToUserUrl
from JianshuResearchTools.rank import GetDailyArticleRankData
from utils.register import task_func
from utils.saver import Saver
from utils.time_helper import (get_now_without_mileseconds,
                               get_today_in_datetime_obj)


def data_iterator() -> Generator:
    data_part = GetDailyArticleRankData()
    for item in data_part:
        yield item
    return


def data_processor(saver: Saver) -> None:
    for item in data_iterator():
        data = {
            "date": get_today_in_datetime_obj(),
            "ranking": item["ranking"],
            "user": {
                "name": item["name"],
                "url": UserSlugToUserUrl(item["uslug"])
            },
            "days": item["check_in_count"]
        }

        saver.add_data(data)

    saver.final_save()


@task_func(
    task_name="简书日更排行榜",
    cron="0 0 12 1/1 * *",
    db_name="daily_update_rank",
    data_bulk_size=100
)
def main(saver: Saver):
    start_time = get_now_without_mileseconds()

    data_processor(saver)

    stop_time = get_now_without_mileseconds()
    cost_time = (stop_time - start_time).total_seconds()

    return (True, saver.get_data_count(), cost_time, "")
