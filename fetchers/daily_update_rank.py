from typing import Generator

from db_manager import GetCollection
from JianshuResearchTools.convert import UserSlugToUserUrl
from JianshuResearchTools.rank import GetDailyArticleRankData
from register import TaskFunc
from utils import GetNowWithoutMileseconds, GetTodayInDatetimeObj

data_collection = GetCollection("daily_update_rank")


def DataGenerator() -> Generator:
    data_part = GetDailyArticleRankData()
    for item in data_part:
        yield item
    return


@TaskFunc("简书日更排行榜", "0 0 12 1/1 * ? *")
def main():
    start_time = GetNowWithoutMileseconds()

    temp = []
    data_count = 0
    for item in DataGenerator():
        data = {
            "date": GetTodayInDatetimeObj(),
            "ranking": item["ranking"],
            "user": {
                "name": item["name"],
                "url": UserSlugToUserUrl(item["uslug"])
            },
            "days": item["ckeck_in_count"]
        }

        temp.append(data)
        data_count += 1

        if len(temp) == 100:
            data_collection.insert_many(temp)
            print(1)
            temp.clear()

    if temp:
        data_collection.insert_many(temp)
        temp.clear()

    stop_time = GetNowWithoutMileseconds()
    cost_time = (stop_time - start_time).total_seconds()

    return (True, data_count, cost_time, "")
