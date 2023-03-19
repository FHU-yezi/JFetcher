from time import sleep
from typing import List

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.schedulers.background import BackgroundScheduler

from event_callbacks import on_error_event, on_executed_event
from fetchers._base import Fetcher
from utils.config import config
from utils.log import run_logger
from utils.module_finder import get_all_fetchers

scheduler = BackgroundScheduler()
scheduler.add_listener(on_executed_event, EVENT_JOB_EXECUTED)
scheduler.add_listener(on_error_event, EVENT_JOB_ERROR)
run_logger.debug("已注册事件回调")

fetchers: List[Fetcher] = [x() for x in get_all_fetchers(config.fetchers.base_path)]
for fetcher in fetchers:
    scheduler.add_job(
        fetcher.run,
        "cron",
        id=fetcher.task_name,
        **fetcher.fetch_time_cron_kwargs,
    )
run_logger.info(
    "已添加获取任务",
    fetchers_name=[fetcher.task_name for fetcher in fetchers],
    fetchers_count=len(fetchers),
)

scheduler.start()
run_logger.info("调度器启动成功")

while True:
    sleep(180)
