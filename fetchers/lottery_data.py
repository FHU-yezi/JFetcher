from datetime import datetime, timedelta
from typing import Dict, Generator

from httpx import get as httpx_get
from JianshuResearchTools.convert import UserSlugToUserUrl

from fetchers._base import Fetcher
from saver import Saver
from utils.retry import retry_on_timeout


def get_lottery_data():
    url = "https://www.jianshu.com/asimov/ad_rewards/winner_list"
    params = {
        "count": 500,
    }
    response = httpx_get(
        url,
        params=params,
        timeout=20,
    )

    return response.json()


get_lottery_data = retry_on_timeout(get_lottery_data)


class LotteryDataFetcher(Fetcher):
    def __init__(self) -> None:
        self.task_name = "简书大转盘抽奖"
        self.fetch_time_cron = "0 0 2,9,14,21 1/1 * *"
        self.collection_name = "lottery_data"
        self.bulk_size = 100

    def should_fetch(self, saver: Saver) -> bool:
        return not saver.is_in_db(
            {
                "time": {
                    "$gt": datetime.now() - timedelta(minutes=5),
                },
            },
        )

    def iter_data(self) -> Generator[Dict, None, None]:
        for item in get_lottery_data():
            yield item

    def process_data(self, data: Dict) -> Dict:
        return {
            "_id": data["id"],
            "time": datetime.fromtimestamp(data["created_at"]),
            "reward_name": data["name"],
            "user": {
                "id": data["user"]["id"],
                "url": UserSlugToUserUrl(data["user"]["slug"]),
                "name": data["user"]["nickname"],
            },
        }

    def should_save(self, data: Dict, saver: Saver) -> bool:
        return not saver.is_in_db({"_id": data["_id"]})

    def save_data(self, data: Dict, saver: Saver) -> None:
        saver.add_one(data)

    def is_success(self, saver: Saver) -> bool:
        return saver.is_in_db(
            {
                "time": {
                    "$gt": datetime.now() - timedelta(minutes=5),
                },
            },
        )
