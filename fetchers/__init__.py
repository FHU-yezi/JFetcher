from fetchers import (article_FP_rank, assets_rank, daily_update_rank,
                      lottery_data)


def init_tasks() -> None:
    """初始化任务

    也可从此位置通过调用某个模块的 main 函数，手动运行该任务
    """
    article_FP_rank
    assets_rank
    daily_update_rank
    lottery_data
