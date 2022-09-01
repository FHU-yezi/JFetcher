from queue import Queue
from threading import Thread
from time import sleep
from typing import Dict, Generator

from db_manager import GetCollection
from JianshuResearchTools.convert import UserSlugToUserUrl
from JianshuResearchTools.exceptions import APIError, ResourceError
from JianshuResearchTools.objects import User, set_cache_status
from JianshuResearchTools.rank import GetAssetsRankData
from log_manager import AddRunLog
from register import TaskFunc
from utils import GetNowWithoutMileseconds, GetTodayInDatetimeObj

set_cache_status(False)

DATA_SAVE_CHECK_INTERVAL = 5
DATA_SAVE_THRESHOLD = 50

data_collection = GetCollection("assets_rank")
data_queue: "Queue[Dict]" = Queue()
is_finished = False
data_count = 0


def DataGenerator(total_count: int) -> Generator:
    now = 1
    while True:
        data_part = GetAssetsRankData(now)
        for item in data_part:
            yield item
            now += 1
            if now > total_count:
                return


def DataProcessor() -> None:
    for item in DataGenerator(1000):
        if not item["uid"]:  # 用户账号状态异常，相关信息无法获取
            AddRunLog("FETCHER", "WARNING", f"排名为 {item['ranking']} "
                      "的用户账号状态异常，无法获取数据，已自动跳过")
            data = {
                "date": GetTodayInDatetimeObj(),
                "ranking": item["ranking"],
                "user": {
                    "id": None,
                    "url": None,
                    "name": None
                },
                "assets": {
                    "FP": item["FP"],
                    "FTN": None,
                    "total": None
                }
            }
        else:
            data = {
                "date": GetTodayInDatetimeObj(),
                "ranking": item["ranking"],
                "user": {
                    "id": item["uid"],
                    "url": UserSlugToUserUrl(item["uslug"]),
                    "name": item["name"]
                },
                "assets": {
                    "FP": item["FP"],
                    "FTN": None,
                    "total": None
                }
            }

            try:
                user = User.from_slug(item["uslug"])
                data["assets"]["FTN"] = user.FTN_count
                data["assets"]["total"] = round(
                    data["assets"]["FP"] + data["assets"]["FTN"], 3
                )
            except (ResourceError, APIError):
                AddRunLog("FETCHER", "WARNING", f"无法获取 id 为 {item['uid']} 的用户"
                          "的简书贝和总资产信息，已自动跳过")
                pass

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


@TaskFunc("简书资产排行榜", "0 0 12 1/1 * *")
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
