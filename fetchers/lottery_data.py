from datetime import datetime, timedelta
from typing import Dict, Generator, List

from httpx import get as httpx_get
from JianshuResearchTools.convert import UserSlugToUserUrl

from constants import NoticePolicy
from fetchers._base import Fetcher
from saver import Saver
from utils.retry import retry_on_network_error


class LotteryDataFetcher(Fetcher):
    def __init__(self) -> None:
        self.task_name = "简书大转盘抽奖"
        self.fetch_time_cron = "0 0 2,9,14,21 1/1 * *"
        self.collection_name = "lottery_data"
        self.bulk_size = 100
        self.notice_policy = NoticePolicy.ALWAYS

    @retry_on_network_error
    def get_lottery_data(self) -> List[Dict]:
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

    def should_fetch(self, saver: Saver) -> bool:
        return not saver.is_in_db(
            {
                "time": {
                    "$gt": datetime.now() - timedelta(hours=3),
                },
            },
        )

    def iter_data(self) -> Generator[Dict, None, None]:
        yield from self.get_lottery_data()

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
        return saver.data_count != 0
