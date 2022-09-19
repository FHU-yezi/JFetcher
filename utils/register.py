from functools import wraps
from typing import Callable, List, Tuple

from utils.log import run_logger

_registered_funcs: List[Tuple[str, str, Callable]] = []


def task_func(task_name: str, cron: str) -> Callable:
    """将函数注册为任务函数

    Args:
        task_name (str): 任务名称
        cron (str): 运行规则 cron 表达式

    Returns:
        Callable: 原函数
    """
    def outer(func: Callable):
        @wraps(func)
        def inner(task_name, cron):
            _registered_funcs.append((task_name, cron, func))
            run_logger.debug("REGISTER", f"成功注册任务函数 {task_name}，"
                             f"cron 表达式：{cron}")
            return func
        return inner(task_name, cron)
    return outer


def get_all_registered_funcs() -> List[Tuple[str, str, Callable]]:
    """获取注册的任务函数列表

    Returns:
        List[Tuple[str, str, str, Callable]]: 注册的任务函数列表
    """
    return _registered_funcs
