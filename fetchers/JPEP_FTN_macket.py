from datetime import datetime, timedelta
from typing import Any, Dict, Generator, Literal

from httpx import post as httpx_post
from sspeedup.time_helper import get_now_without_mileseconds

from constants import NoticePolicy
from fetchers._base import Fetcher
from saver import Saver
from utils.retry import retry_on_network_error


class AssetsRankFetcher(Fetcher):
    def get_fetch_time(self) -> datetime:
        """保证数据获取时间对齐十分钟间隔
        """
        result = get_now_without_mileseconds()
        result = result.replace(second=0)
        while result.minute % 10 != 0:
            result = result - timedelta(minutes=1)
        return result

    @retry_on_network_error
    def get_FTN_macket_data(self, type_: Literal["buy", "sell"]) -> Generator[Dict, None, None]:  # noqa: N802
        page = 1
        while True:
            url = "https://20221023.tp.lanrenmb.net/api/getList/furnish.bei/"
            params: Dict[str, Any] = {
                "page": page,
            }

            body_json: Dict[str, Any] = {
                "filter": [
                    {"trade": 1 if type_ == "buy" else 0},
                    {"status": 1},
                    {"finish": 0},
                ],
                "sort": "price,pub_date" if type_ == "buy" else "-price,pub_date",
                "bind": [
                    {
                        "member.user": {
                            "addField": [
                                {"username_md5": "username_md5"},
                            ],
                            "filter": [
                                {"id": r"{{uid}}"},
                            ],
                            "fields": "id,username",
                        }
                    }
                ],
                "addField": [
                    {"transactions_count": "tradeCount"},
                    {"traded": "tradeNum"},
                    {"remaining": "leftNum"},
                    {"tradable": "tradable"},
                ],
            }

            response = httpx_post(url, params=params, json=body_json)
            if response.status_code != 200:
                raise ValueError()
            response_json = response.json()
            if response_json["code"] != 200:
                raise ValueError()
            if not response_json["data"]:
                return None

            yield from response_json["data"]

            page += 1

    def __init__(self) -> None:
        self.task_name = "简书积分兑换平台-贝市"
        self.fetch_time_cron = "0 0/10 * * * *"
        self.collection_name = "JPEP_FTN_macket"
        self.bulk_size = 100
        self.notice_policy = NoticePolicy.ONLY_FAILED

    def should_fetch(self, saver: Saver) -> bool:
        del saver
        return True

    def iter_data(self) -> Generator[Dict, None, None]:
        yield from self.get_FTN_macket_data(type_="buy")
        yield from self.get_FTN_macket_data(type_="sell")

    def process_data(self, data: Dict) -> Dict:
        return {
            "fetch_time": self.get_fetch_time(),
            "order_id": data["id"],
            "trade_type": "buy" if data["trade"] == 1 else "sell",
            "publish_time": datetime.fromisoformat(data["pub_date"]),
            "is_anonymous": data["anony"] == 1,
            "price": data["price"],
            "amount": {
                "total": data["totalNum"],
                "traded": data["traded"],
                "remaining": data["remaining"],
                "tradable": data["tradable"],
            },
            "traded_percentage": round(data["traded"] / data["totalNum"], 2),
            "minimum_trade_count": data["minNum"],
            "transactions_count": data["transactions_count"],
            "user": {
                "id": data["member.user"][0]["id"],
                "name": data["member.user"][0]["username"],
                "name_md5": data["member.user"][0]["username_md5"],
            }
        }

    def should_save(self, data: Dict, saver: Saver) -> bool:
        del data
        del saver
        return True

    def save_data(self, data: Dict, saver: Saver) -> None:
        saver.add_one(data)

    def is_success(self, saver: Saver) -> bool:
        del saver
        return True
