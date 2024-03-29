from datetime import timedelta
from typing import Dict, Generator, Optional, Tuple

from httpx import get as httpx_get
from JianshuResearchTools.convert import (
    ArticleSlugToArticleUrl,
    UserSlugToUserUrl,
)
from JianshuResearchTools.rank import GetArticleFPRankData
from sspeedup.time_helper import get_today_in_datetime_obj

from constants import NoticePolicy
from fetchers._base import Fetcher
from saver import Saver
from utils.log import run_logger
from utils.retry import retry_on_network_error

GetArticleFPRankData = retry_on_network_error(GetArticleFPRankData)


class ArticleFPRankFetcher(Fetcher):
    def __init__(self) -> None:
        self.task_name = "简书文章收益排行榜"
        self.fetch_time_cron = "0 0 1 1/1 * *"
        self.collection_name = "article_FP_rank"
        self.bulk_size = 100
        self.notice_policy = NoticePolicy.ALWAYS

    def get_user_id_url_from_article_slug(
        self, article_slug: str
    ) -> Tuple[Optional[int], Optional[str]]:
        response = retry_on_network_error(httpx_get)(
            f"https://www.jianshu.com/asimov/p/{article_slug}"
        )
        result = response.json()

        # 如果作者被封无法获取数据，则跳过采集
        if "error" in result:
            return (None, None)

        return (result["user"]["id"], UserSlugToUserUrl(result["user"]["slug"]))

    def should_fetch(self, saver: Saver) -> bool:
        return not saver.is_in_db(
            {"date": get_today_in_datetime_obj() - timedelta(days=1)}
        )

    def iter_data(self) -> Generator[Dict, None, None]:
        yield from GetArticleFPRankData("latest")

    def process_data(self, data: Dict) -> Dict:
        result = {
            "date": get_today_in_datetime_obj() - timedelta(days=1),
            "ranking": data["ranking"],
            "article": {
                "title": None,
                "url": None,
            },
            "author": {
                "id": None,
                "url": None,
                "name": None,
            },
            "reward": {
                "to_author": data["fp_to_author"],
                "to_voter": data["fp_to_voter"],
                "total": data["total_fp"],
            },
        }
        if not data["aslug"]:  # 文章被删除导致相关信息无法访问
            run_logger.warning(
                "文章被删除，部分数据无法采集，已自动跳过",
                task_name=self.task_name,
                ranking=data["ranking"],
            )
            return result

        result["article"]["title"] = data["title"]
        result["article"]["url"] = ArticleSlugToArticleUrl(data["aslug"])
        result["author"]["name"] = data["author_name"]

        uid, user_url = self.get_user_id_url_from_article_slug(data["aslug"])
        result["author"]["id"] = uid
        result["author"]["url"] = user_url

        return result

    def should_save(self, data: Dict, saver: Saver) -> bool:
        del data
        del saver
        return True

    def save_data(self, data: Dict, saver: Saver) -> None:
        saver.add_one(data)

    def is_success(self, saver: Saver) -> bool:
        return saver.is_in_db({"date": get_today_in_datetime_obj() - timedelta(days=1)})
