from time import sleep

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.schedulers.background import BackgroundScheduler

from event_handlers import on_fail_event, on_success_event
from fetchers import init_tasks
from utils.log import run_logger
from utils.register import get_all_registered_funcs
from utils.saver import Saver
from utils.time_helper import cron_to_kwargs

init_tasks()  # 运行相关模块，继而对采集任务进行注册操作

scheduler = BackgroundScheduler()
run_logger.info("SYSTEM", "成功初始化调度器")

funcs = get_all_registered_funcs()
run_logger.info("SYSTEM", f"获取到 {len(funcs)} 个任务函数")

for func, task_name, cron, db_name, data_bulk_size in funcs:
    saver: Saver = Saver(db_name, data_bulk_size)
    scheduler.add_job(func, "cron", id=task_name, **cron_to_kwargs(cron),
                      kwargs={"saver": saver})
    func(saver)
run_logger.info("SYSTEM", "已将任务函数加入调度")

scheduler.add_listener(on_success_event, EVENT_JOB_EXECUTED)
scheduler.add_listener(on_fail_event, EVENT_JOB_ERROR)
run_logger.info("SYSTEM", "成功注册事件回调")

scheduler.start()
run_logger.info("SYSTEM", "调度器已启动")

while True:
    sleep(10)
