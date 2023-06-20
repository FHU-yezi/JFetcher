from typing import Dict, Generator, Set

from JianshuResearchTools.convert import (
    ArticleSlugToArticleUrl,
    UserSlugToUserUrl,
)
from JianshuResearchTools.objects import Collection, set_cache_status
from sspeedup.time_helper import get_today_in_datetime_obj

from constants import NoticePolicy
from fetchers._base import Fetcher
from saver import Saver
from utils.log import run_logger
from utils.retry import retry_on_network_error

set_cache_status(False)


class LPCollectionsFetcher(Fetcher):
    def __init__(self) -> None:
        self.task_name = "LP 理事会相关专题"
        self.fetch_time_cron = "0 0 0 1/1 * *"
        self.collection_name = "LP_collections"
        self.bulk_size = 60
        self.notice_policy = NoticePolicy.ALWAYS

        self.collections_to_fetch: Set[Collection] = {
            Collection.from_url("https://www.jianshu.com/c/f61832508891"),  # 理事会点赞汇总
        }

    def should_fetch(self, saver: Saver) -> bool:
        del saver
        return True

    def _iter_one_collection(
        self, collection: Collection
    ) -> Generator[Dict, None, None]:
        collection.articles_info = retry_on_network_error(collection.articles_info)

        page: int = 1
        total_count: int = 0
        while True:
            data_part = collection.articles_info(page=page, count=40)
            page += 1

            if not data_part:
                return
            for item in data_part:
                yield item
                total_count += 1
                if total_count == 120:
                    return

    def iter_data(self) -> Generator[Dict, None, None]:
        for collection in self.collections_to_fetch:
            # TODO
            run_logger.info("开始获取 理事会点赞汇总（https://www.jianshu.com/c/f61832508891）的数据")
            for item in self._iter_one_collection(collection):
                yield item

    def process_data(self, data: Dict) -> Dict:
        return {
            "fetch_date": get_today_in_datetime_obj(),
            "from_collection": "理事会点赞汇总",
            "article": {
                "id": data["aid"],
                "url": ArticleSlugToArticleUrl(data["aslug"]),
                "title": data["title"],
                "release_time": data["release_time"],
                "views_count": data["views_count"],
                "likes_count": data["likes_count"],
                "comments_count": data["comments_count"],
                "rewards_count": data["rewards_count"],
                "total_FP_amount": data["total_fp_amount"],
                "is_paid": data["paid"],
                "is_commentable": data["commentable"],
                "summary": data["summary"],
            },
            "author": {
                "id": data["user"]["uid"],
                "url": UserSlugToUserUrl(data["user"]["uslug"]),
                "name": data["user"]["name"],
            },
        }

    def should_save(self, data: Dict, saver: Saver) -> bool:
        return not saver.is_in_db(
            {
                "article.id": data["article"]["id"],
            },
        )

    def save_data(self, data: Dict, saver: Saver) -> None:
        saver.add_one(data)

    def is_success(self, saver: Saver) -> bool:
        return saver.is_in_db({"fetch_date": get_today_in_datetime_obj()})
