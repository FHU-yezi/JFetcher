from queue import Queue
from threading import Thread
from time import sleep
from typing import Dict, Generator

from db_manager import GetCollection
from JianshuResearchTools.convert import ArticleSlugToArticleUrl
from JianshuResearchTools.rank import GetArticleFPRankData
from log_manager import AddRunLog
from register import TaskFunc
from utils import GetNowWithoutMileseconds, GetTodayInDatetimeObj

DATA_SAVE_CHECK_INTERVAL = 3
DATA_SAVE_THRESHOLD = 100

data_collection = GetCollection("article_FP_rank")
data_queue: "Queue[Dict]" = Queue()
is_finished = False
data_count = 0


def DataGenerator() -> Generator:
    data_part = GetArticleFPRankData("latest")
    for item in data_part:
        yield item
    return


def DataProcessor() -> None:
    for item in DataGenerator():
        AddRunLog("FETCHER", "WARNING", f"排名为 {item['ranking']} "
                  "的文章被删除，无法采集数据，已自动跳过")
        if not item["author_name"]:  # 文章被删除导致相关信息无法访问
            data = {
                "date": GetTodayInDatetimeObj(),
                "ranking": item["ranking"],
                "article": {
                    "title": None,
                    "url": None,
                },
                "author": {
                    "name": None
                },
                "reward": {
                    "to_author": item["fp_to_author"],
                    "to_voter": item["fp_to_voter"],
                    "total": item["total_fp"],
                }
            }
        else:
            data = {
                "date": GetTodayInDatetimeObj(),
                "ranking": item["ranking"],
                "article": {
                    "title": item["title"],
                    "url": ArticleSlugToArticleUrl(item["aslug"]),
                },
                "author": {
                    "name": item["author_name"]
                },
                "reward": {
                    "to_author": item["fp_to_author"],
                    "to_voter": item["fp_to_voter"],
                    "total": item["total_fp"],
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


@TaskFunc("简书文章收益排行榜", "0 0 8 1/1 * *")
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
