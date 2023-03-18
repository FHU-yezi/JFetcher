from typing import Dict, Generator

from httpx import TimeoutException
from JianshuResearchTools.convert import UserSlugToUserUrl
from JianshuResearchTools.exceptions import APIError, ResourceError
from JianshuResearchTools.objects import User
from JianshuResearchTools.rank import GetAssetsRankData
from sspeedup.time_helper import get_today_in_datetime_obj

from constants import NoticePolicy
from fetchers._base import Fetcher
from saver import Saver
from utils.log import run_logger
from utils.retry import retry_on_network_error

GetAssetsRankData = retry_on_network_error(GetAssetsRankData)


class AssetsRankFetcher(Fetcher):
    def __init__(self) -> None:
        self.task_name = "简书资产排行榜"
        self.fetch_time_cron = "0 0 12 1/1 * *"
        self.collection_name = "assets_rank"
        self.bulk_size = 100
        self.notice_policy = NoticePolicy.ALWAYS

    def should_fetch(self, saver: Saver) -> bool:
        return not saver.is_in_db({"date": get_today_in_datetime_obj()})

    def iter_data(self) -> Generator[Dict, None, None]:
        total_count = 1000
        now = 1
        while True:
            data_part = GetAssetsRankData(now)
            for item in data_part:
                yield item
                now += 1
                if now > total_count:
                    return

    def process_data(self, data: Dict) -> Dict:
        result = {
            "date": get_today_in_datetime_obj(),
            "ranking": data["ranking"],
            "user": {
                "id": None,
                "url": None,
                "name": None,
            },
            "assets": {
                "FP": None,
                "FTN": None,
                # JRT 写错了
                "total": data["FP"],
            },
        }
        if not data["uid"]:  # 用户账号状态异常，相关信息无法获取
            run_logger.warning(
                "用户账号状态异常，部分数据无法采集，已自动跳过",
                task_name=self.task_name,
                ranking=data["ranking"],
            )
            return result

        result["user"]["id"] = data["uid"]
        result["user"]["url"] = UserSlugToUserUrl(data["uslug"])
        result["user"]["name"] = data["name"]

        try:
            user = User.from_slug(data["uslug"])
            result["assets"]["FP"] = user.FP_count
            result["assets"]["FTN"] = round(
                result["assets"]["total"] - result["assets"]["FP"], 3
            )
        except (ResourceError, APIError, TimeoutException):
            run_logger.warning(
                "简书贝和总资产信息获取失败，已自动跳过",
                task_name=self.task_name,
                user_id=data["uid"],
            )

        return result

    def should_save(self, data: Dict, saver: Saver) -> bool:
        del data
        del saver
        return True

    def save_data(self, data: Dict, saver: Saver) -> None:
        saver.add_one(data)

    def is_success(self, saver: Saver) -> bool:
        return saver.is_in_db({"date": get_today_in_datetime_obj()})
