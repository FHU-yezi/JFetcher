from log_manager import AddRunLog
from message_sender import SendTaskFailtureCard, SendTaskSuccessCard
from utils import CostTimeToString


def TaskSuccess(task_name: str, data_count: int, cost_time: int) -> None:
    """任务成功回调

    Args:
        task_name (str): 任务名称
        data_count (int): 采集的数据数量
        cost_time (int): 耗时，单位为秒
    """
    cost_time_str = CostTimeToString(cost_time)
    AddRunLog("FETCHER", "DEBUG", f"{task_name} 运行成功，"
              f"采集的数据量：{data_count}，耗时：{cost_time}")

    SendTaskSuccessCard(task_name, data_count, cost_time_str)


def TaskFailure(task_name: str, error_message: str) -> None:
    """任务失败回调

    Args:
        task_name (str): 任务名称
        error_message (str): 错误信息
    """
    AddRunLog("FETCHER", "DEBUG", f"{task_name} 运行失败，"
              f"错误信息：{error_message}")

    SendTaskFailtureCard(task_name, error_message)
