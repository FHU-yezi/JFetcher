from queue import Queue
from threading import Thread
from time import sleep
from typing import Dict, Generator

from JianshuResearchTools.convert import UserSlugToUserUrl
from JianshuResearchTools.rank import GetDailyArticleRankData
from utils.db import get_collection
from utils.register import task_func
from utils.time_helper import (get_now_without_mileseconds,
                               get_today_in_datetime_obj)

DATA_SAVE_CHECK_INTERVAL = 1
DATA_SAVE_THRESHOLD = 100

data_collection = get_collection("daily_update_rank")
data_queue: "Queue[Dict]" = Queue()
is_finished = False
data_count = 0


def DataGenerator() -> Generator:
    data_part = GetDailyArticleRankData()
    for item in data_part:
        yield item
    return


def DataProcessor() -> None:
    for item in DataGenerator():
        data = {
            "date": get_today_in_datetime_obj(),
            "ranking": item["ranking"],
            "user": {
                "name": item["name"],
                "url": UserSlugToUserUrl(item["uslug"])
            },
            "days": item["check_in_count"]
        }

        data_queue.put(data)


def DataSaver() -> None:
    global data_count

    while not is_finished:
        if data_queue.qsize() < DATA_SAVE_THRESHOLD:
            sleep(DATA_SAVE_CHECK_INTERVAL)
            continue

        data_to_save = [data_queue.get()
                        for _ in range(DATA_SAVE_THRESHOLD)]
        data_collection.insert_many(data_to_save)
        data_count += len(data_to_save)

    # 采集完成，存储剩余数据
    if data_queue.qsize() > 0:
        data_to_save = [data_queue.get() for _ in range(data_queue.qsize())]
        data_collection.insert_many(data_to_save)
        data_count += len(data_to_save)


@task_func("简书日更排行榜", "0 0 12 1/1 * *")
def main():
    global data_count
    global is_finished

    data_count = 0
    is_finished = False

    start_time = get_now_without_mileseconds()

    saver = Thread(target=DataSaver)
    saver.start()
    DataProcessor()
    is_finished = True
    saver.join()

    stop_time = get_now_without_mileseconds()
    cost_time = (stop_time - start_time).total_seconds()

    return (True, data_count, cost_time, "")
