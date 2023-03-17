from typing import Dict, Generator

from JianshuResearchTools.convert import UserSlugToUserUrl
from JianshuResearchTools.rank import GetDailyArticleRankData
from sspeedup.time_helper import get_today_in_datetime_obj

from fetchers._base import Fetcher
from saver import Saver
from utils.retry import retry_on_network_error

GetDailyArticleRankData = retry_on_network_error(GetDailyArticleRankData)


class DailyUpdateRankFetcher(Fetcher):
    def __init__(self) -> None:
        self.task_name = "简书日更排行榜"
        self.fetch_time_cron = "0 0 12 1/1 * *"
        self.collection_name = "daily_update_rank"
        self.bulk_size = 100

    def should_fetch(self, saver: Saver) -> bool:
        return not saver.is_in_db({"date": get_today_in_datetime_obj()})

    def iter_data(self) -> Generator[Dict, None, None]:
        yield from GetDailyArticleRankData()

    def process_data(self, data: Dict) -> Dict:
        return {
            "date": get_today_in_datetime_obj(),
            "ranking": data["ranking"],
            "user": {
                "name": data["name"],
                "url": UserSlugToUserUrl(data["uslug"]),
            },
            "days": data["check_in_count"],
        }

    def should_save(self, data: Dict, saver: Saver) -> bool:
        del data
        del saver
        return True

    def save_data(self, data: Dict, saver: Saver) -> None:
        saver.add_one(data)

    def is_success(self, saver: Saver) -> bool:
        return saver.is_in_db({"date": get_today_in_datetime_obj()})
