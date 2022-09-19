from callbacks import task_fail, task_success


def on_success_event(event) -> None:
    """调度任务成功事件回调

    Args:
        event: 事件对象
    """
    task_name = event.job_id
    success, data_count, cost_time, error_message = event.retval

    if success:
        task_success(task_name, data_count, cost_time)
    else:
        task_fail(task_name, error_message)


def on_fail_event(event) -> None:
    """调度任务失败事件回调

    Args:
        event: 事件对象
    """
    task_name = event.job_id
    error_message = "未捕获异常"

    task_fail(task_name, error_message)
