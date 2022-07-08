from typing import Generator

from db_manager import GetCollection
from JianshuResearchTools.convert import UserSlugToUserUrl
from JianshuResearchTools.exceptions import APIError, ResourceError
from JianshuResearchTools.objects import User, set_cache_status
from JianshuResearchTools.rank import GetAssetsRankData
from register import TaskFunc
from utils import GetNowWithoutMileseconds, GetTodayInDatetimeObj

set_cache_status(False)

data_collection = GetCollection("assets_rank")


def DataGenerator(total_count: int) -> Generator:
    now = 1
    while True:
        data_part = GetAssetsRankData(now)
        for item in data_part:
            yield item
            now += 1
            if now > total_count:
                return


@TaskFunc("简书资产排行榜", "0 0 12 1/1 * ?")
def main():
    start_time = GetNowWithoutMileseconds()

    temp = []
    data_count = 0
    for item in DataGenerator(1000):
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
            data["assets"]["total"] = round(data["assets"]["FP"] + data["assets"]["FTN"], 3)
        except (ResourceError, APIError):
            # TODO: 记录日志
            pass

        temp.append(data)
        data_count += 1

        if len(temp) == 50:
            data_collection.insert_many(temp)
            temp.clear()

    if temp:
        data_collection.insert_many(temp)
        temp.clear()

    stop_time = GetNowWithoutMileseconds()
    cost_time = (stop_time - start_time).total_seconds()

    return (True, data_count, cost_time, "")
