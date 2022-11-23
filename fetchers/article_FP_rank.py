from datetime import timedelta
from typing import Generator, Tuple

from httpx import get as httpx_get
from JianshuResearchTools.convert import (
    ArticleSlugToArticleUrl,
    ArticleUrlToArticleSlug,
    UserSlugToUserUrl,
)
from JianshuResearchTools.rank import GetArticleFPRankData
from utils.log import run_logger
from utils.register import task_func
from utils.saver import Saver
from utils.time_helper import (
    get_now_without_mileseconds,
    get_today_in_datetime_obj,
)


def get_id_url_from_article_url(article_url: str) -> Tuple[int, str]:
    aslug = ArticleUrlToArticleSlug(article_url)
    response = httpx_get(f"https://www.jianshu.com/asimov/p/{aslug}")
    result = response.json()
    if result.get("error"):
        raise Exception("文章已被删除")
    return (result["user"]["id"], UserSlugToUserUrl(result["user"]["slug"]))


def data_iterator() -> Generator:
    data_part = GetArticleFPRankData("latest")
    for item in data_part:
        yield item
    return


def data_processor(saver: Saver) -> None:
    for item in data_iterator():
        data = {
            "date": get_today_in_datetime_obj() - timedelta(days=1),  # 昨天
            "ranking": item["ranking"],
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
                "to_author": item["fp_to_author"],
                "to_voter": item["fp_to_voter"],
                "total": item["total_fp"],
            },
        }
        if not item["author_name"]:  # 文章被删除导致相关信息无法访问
            run_logger.warning(f"排名为 {item['ranking']} 的文章被删除，部分数据无法采集，已自动跳过")
        else:
            data["article"]["title"] = item["title"]
            data["article"]["url"] = ArticleSlugToArticleUrl(item["aslug"])
            data["author"]["name"] = item["author_name"]

            try:
                uid, user_url = get_id_url_from_article_url(data["article"]["url"])
                data["author"]["id"] = uid
                data["author"]["url"] = user_url
            except Exception:  # 无法获取信息
                pass

        saver.add_data(data)

    saver.final_save()


@task_func(
    task_name="简书文章收益排行榜",
    cron="0 0 1 1/1 * *",
    db_name="article_FP_rank",
    data_bulk_size=100,
)
def main(saver: Saver):
    start_time = get_now_without_mileseconds()

    data_processor(saver)

    stop_time = get_now_without_mileseconds()
    cost_time = (stop_time - start_time).total_seconds()

    return (True, saver.get_data_count(), cost_time, "")
