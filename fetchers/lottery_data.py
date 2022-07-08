from datetime import datetime
from typing import Generator

from db_manager import GetCollection
from httpx import get as httpx_get
from JianshuResearchTools.convert import UserSlugToUserUrl
from register import TaskFunc
from utils import GetNowWithoutMileseconds

data_collection = GetCollection("lottery_data")


def DataGenerator() -> Generator:
    params = {
        "count": 500
    }
    response = httpx_get("https://www.jianshu.com/asimov/ad_rewards/winner_list",
                         params=params, timeout=20)
    try:
        response.raise_for_status()
    except Exception:
        # TODO: 失败并记录日志
        pass

    data_part = response.json()
    for item in data_part:
        yield item
    return


def GetLastSavedItemID() -> int:
    if data_collection.count_documents({}) == 0:
        # 没有数据
        return 0  # 保证所有数据都被存储

    return data_collection.find_one(sort=[("_id", -1)])["_id"]


@TaskFunc("简书大转盘抽奖", "0 0 2,9,14,21 1/1 * *")
def main():
    start_time = GetNowWithoutMileseconds()

    temp = []
    data_count = 0
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

        temp.append(data)

    last_id = GetLastSavedItemID()
    temp = [x for x in temp if x["_id"] > last_id]
    if data_collection:
        data_collection.insert_many(temp)

    data_count = len(temp)

    stop_time = GetNowWithoutMileseconds()
    cost_time = (stop_time - start_time).total_seconds()

    return (True, data_count, cost_time, "")
