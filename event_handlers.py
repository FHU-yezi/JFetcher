from callbacks import TaskFailure, TaskSuccess


def OnSuccessEvent(event) -> None:
    """调度任务成功事件回调

    Args:
        event: 事件对象
    """
    task_name = event.job_id
    success, data_count, cost_time, error_message = event.retval

    if success:
        TaskSuccess(task_name, data_count, cost_time)
    else:
        TaskFailure(task_name, error_message)


def OnFailureEvent(event) -> None:
    """调度任务失败事件回调

    Args:
        event: 事件对象
    """
    task_name = event.job_id
    error_message = "未捕获异常"

    TaskFailure(task_name, error_message)
