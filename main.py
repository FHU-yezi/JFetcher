from time import sleep

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.schedulers.background import BackgroundScheduler

from event_handlers import OnFailureEvent, OnSuccessEvent
from fetchers import init_tasks
from log_manager import AddRunLog
from register import GetAllRegisteredFuncs
from utils import CronToKwargs

init_tasks()  # 运行相关模块，继而对采集任务进行注册操作

scheduler = BackgroundScheduler()
AddRunLog("SYSTEM", "INFO", "成功初始化调度器")

funcs = GetAllRegisteredFuncs()
AddRunLog("SYSTEM", "INFO", f"获取到 {len(funcs)} 个任务函数")

for task_name, cron, func in funcs:
    scheduler.add_job(func, "cron", **CronToKwargs(cron), id=task_name)
AddRunLog("SYSTEM", "INFO", "已将任务函数加入调度")

scheduler.add_listener(OnSuccessEvent, EVENT_JOB_EXECUTED)
scheduler.add_listener(OnFailureEvent, EVENT_JOB_ERROR)
AddRunLog("SYSTEM", "INFO", "成功注册事件回调")

scheduler.start()
AddRunLog("SYSTEM", "INFO", "调度器已启动")

while True:
    sleep(10)
