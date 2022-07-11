from datetime import datetime
from queue import Queue
from threading import Thread
from time import sleep
from typing import Dict, Generator

from db_manager import GetCollection
from httpx import get as httpx_get
from JianshuResearchTools.convert import UserSlugToUserUrl
from log_manager import AddRunLog
from register import TaskFunc
from utils import GetNowWithoutMileseconds

DATA_SAVE_CHECK_INTERVAL = 3
DATA_SAVE_THRESHOLD = 100

data_collection = GetCollection("lottery_data")
data_queue: "Queue[Dict]" = Queue()
is_finished = False
data_count = 0


def GetLastSavedItemID() -> int:
    if data_collection.count_documents({}) == 0:
        # 没有数据
        return 0  # 保证所有数据都被存储

    return data_collection.find_one(sort=[("_id", -1)])["_id"]


def DataGenerator() -> Generator:
    params = {
        "count": 500
    }
    response = httpx_get(
        "https://www.jianshu.com/asimov/ad_rewards/winner_list",
        params=params, timeout=20
    )
    try:
        response.raise_for_status()
    except Exception:
        AddRunLog("FETCHER", "ERROR", "无法获取大转盘抽奖数据")
        return

    data_part = response.json()
    for item in data_part:
        yield item
    return


def DataProcessor() -> None:
    for item in DataGenerator():
        data = {
            "_id": item["id"],
            "time": datetime.fromtimestamp(item["created_at"]),
            "reward_name": item["name"],
            "user": {
                "id": item["user"]["id"],
                "url": UserSlugToUserUrl(item["user"]["slug"]),
                "name": item["user"]["nickname"]
            }
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


@TaskFunc("简书大转盘抽奖", "0 0 2,9,14,21 1/1 * *")
def main():
    global data_count
    global is_finished

    data_count = 0
    is_finished = False

    start_time = GetNowWithoutMileseconds()

    saver = Thread(target=DataSaver)
    saver.start()
    DataProcessor()
    is_finished = True
    saver.join()

    stop_time = GetNowWithoutMileseconds()
    cost_time = (stop_time - start_time).total_seconds()

    return (True, data_count, cost_time, "")
