from datetime import timedelta
from typing import Generator

from JianshuResearchTools.convert import ArticleSlugToArticleUrl
from JianshuResearchTools.rank import GetArticleFPRankData
from utils.log import run_logger
from utils.register import task_func
from utils.saver import Saver
from utils.time_helper import (get_now_without_mileseconds,
                               get_today_in_datetime_obj)


def data_iterator() -> Generator:
    data_part = GetArticleFPRankData("latest")
    for item in data_part:
        yield item
    return


def data_processor(saver: Saver) -> None:
    for item in data_iterator():
        if not item["author_name"]:  # 文章被删除导致相关信息无法访问
            run_logger.warning("FETCHER", f"排名为 {item['ranking']} "
                               "的文章被删除，无法采集数据，已自动跳过")
            data = {
                "date": get_today_in_datetime_obj() - timedelta(days=1),
                "ranking": item["ranking"],
                "article": {
                    "title": None,
                    "url": None,
                },
                "author": {
                    "name": None
                },
                "reward": {
                    "to_author": item["fp_to_author"],
                    "to_voter": item["fp_to_voter"],
                    "total": item["total_fp"],
                }
            }
        else:
            data = {
                "date": get_today_in_datetime_obj() - timedelta(days=1),
                "ranking": item["ranking"],
                "article": {
                    "title": item["title"],
                    "url": ArticleSlugToArticleUrl(item["aslug"]),
                },
                "author": {
                    "name": item["author_name"]
                },
                "reward": {
                    "to_author": item["fp_to_author"],
                    "to_voter": item["fp_to_voter"],
                    "total": item["total_fp"],
                }
            }

        saver.add_data(data)

    saver.final_save()


@task_func(
    task_name="简书文章收益排行榜",
    cron="0 0 1 1/1 * *",
    db_name="article_FP_rank",
    data_bulk_size=100
)
def main(saver: Saver):
    start_time = get_now_without_mileseconds()

    data_processor(saver)

    stop_time = get_now_without_mileseconds()
    cost_time = (stop_time - start_time).total_seconds()

    return (True, saver.get_data_count(), cost_time, "")
