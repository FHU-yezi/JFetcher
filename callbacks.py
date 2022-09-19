from utils.log import run_logger
from utils.message import send_task_fail_card, send_task_success_card
from utils.time_helper import human_readable_time_cost


def task_success(task_name: str, data_count: int, cost_time: int) -> None:
    """任务成功回调

    Args:
        task_name (str): 任务名称
        data_count (int): 采集的数据数量
        cost_time (int): 耗时，单位为秒
    """
    cost_time_str = human_readable_time_cost(cost_time)
    run_logger.debug("FETCHER", f"{task_name} 运行成功，"
                     f"采集的数据量：{data_count}，耗时：{cost_time}")

    send_task_success_card(task_name, data_count, cost_time_str)


def task_fail(task_name: str, error_message: str) -> None:
    """任务失败回调

    Args:
        task_name (str): 任务名称
        error_message (str): 错误信息
    """
    run_logger.debug("FETCHER", f"{task_name} 运行失败，"
                     f"错误信息：{error_message}")

    send_task_fail_card(task_name, error_message)
