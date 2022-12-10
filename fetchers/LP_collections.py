from typing import Generator, Set

from JianshuResearchTools.convert import (
    ArticleSlugToArticleUrl,
    UserSlugToUserUrl,
)
from JianshuResearchTools.objects import Collection, set_cache_status

from utils.log import run_logger
from utils.register import task_func
from utils.saver import Saver
from utils.time_helper import (
    get_now_without_mileseconds,
    get_today_in_datetime_obj,
)

set_cache_status(False)

COLLECTIONS_TO_FETCH: Set[Collection] = {
    Collection.from_url("https://www.jianshu.com/c/f61832508891"),  # 理事会点赞汇总
}


def is_already_in_db(db, article_id: int) -> bool:
    return db.count_documents({"article.id": article_id}) != 0


def data_iterator(collection: Collection) -> Generator:
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
                break


def data_processor(saver: Saver) -> None:
    for collection in COLLECTIONS_TO_FETCH:
        run_logger.info(f"开始获取 {collection.name}（{collection.url}）的数据")
        for item in data_iterator(collection):
            if is_already_in_db(saver._db, item["aid"]):
                continue

            data = {
                "fetch_date": get_today_in_datetime_obj(),
                "from_collection": collection.name,
                "article": {
                    "id": item["aid"],
                    "url": ArticleSlugToArticleUrl(item["aslug"]),
                    "title": item["title"],
                    "release_time": item["release_time"].replace(tzinfo=None),
                    "views_count": item["views_count"],
                    "likes_count": item["likes_count"],
                    "comments_count": item["comments_count"],
                    "rewards_count": item["rewards_count"],
                    "total_FP_amount": item["total_fp_amount"],
                    "is_paid": item["paid"],
                    "is_commentable": item["commentable"],
                    "summary": item["summary"],
                },
                "author": {
                    "id": item["user"]["uid"],
                    "url": UserSlugToUserUrl(item["user"]["uslug"]),
                    "name": item["user"]["name"],
                },
            }

            saver.add_data(data)

    saver.final_save()


@task_func(
    task_name="LP 理事会相关专题",
    cron="0 0 0 1/1 * *",
    db_name="LP_collections",
    data_bulk_size=60,
)
def main(saver: Saver):
    start_time = get_now_without_mileseconds()

    data_processor(saver)

    stop_time = get_now_without_mileseconds()
    cost_time = (stop_time - start_time).total_seconds()

    return (True, saver.get_data_count(), cost_time, "")
