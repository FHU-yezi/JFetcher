from typing import Generator

from db_manager import GetCollection
from JianshuResearchTools.convert import ArticleSlugToArticleUrl
from JianshuResearchTools.rank import GetArticleFPRankData
from register import TaskFunc
from utils import GetNowWithoutMileseconds, GetTodayInDatetimeObj

data_collection = GetCollection("article_FP_rank")


def DataGenerator() -> Generator:
    data_part = GetArticleFPRankData("latest")
    for item in data_part:
        yield item
    return


@TaskFunc("简书文章收益排行榜", "0 0 8 1/1 * *")
def main():
    start_time = GetNowWithoutMileseconds()

    temp = []
    data_count = 0
    for item in DataGenerator():
        if not item["author_name"]:  # 文章被删除导致相关信息无法访问
            data = {
                "date": GetTodayInDatetimeObj(),
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
                "date": GetTodayInDatetimeObj(),
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

        temp.append(data)
        data_count += 1

        if len(temp) == 100:
            data_collection.insert_many(temp)
            temp.clear()

    if temp:
        data_collection.insert_many(temp)
        temp.clear()

    stop_time = GetNowWithoutMileseconds()
    cost_time = (stop_time - start_time).total_seconds()

    return (True, data_count, cost_time, "")
