from typing import Callable, List, Tuple

from utils.log import run_logger
from utils.saver import Saver

_registered_funcs: List[Tuple[Callable[[Saver], None], str, str, str, int]] = []


def task_func(task_name: str, cron: str, db_name: str, data_bulk_size: int) -> Callable:
    """将函数注册为任务函数

    Args:
        task_name (str): 任务名称
        cron (str): 运行规则 cron 表达式
        db_name (str): 数据库名称
        data_bulk_size (int): 数据保存批大小

    Returns:
        Callable: 原函数
    """
    def outer(func: Callable[[Saver], None]):
        _registered_funcs.append((func, task_name, cron, db_name, data_bulk_size))
        run_logger.debug("REGISTER", f"成功注册任务函数 {task_name}")
        return func
    return outer


def get_all_registered_funcs() -> List[Tuple[Callable[[Saver], None], str, str, str, int]]:
    """获取注册的任务函数列表

    Returns:
        List[Tuple[Callable[[Saver], None], str, str, str, int]]: 注册的任务函数列表
    """
    return _registered_funcs
