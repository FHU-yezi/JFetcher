from apscheduler.events import JobExecutionEvent


def on_executed_event(event: JobExecutionEvent):
    pass


def on_error_event(event: JobExecutionEvent):
    pass


def on_task_successed():
    pass


def on_task_failed():
    pass


def on_task_skipped():
    pass
