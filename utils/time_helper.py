COST_TIME_MAPPING = {
    "分": 60,
    "秒": 1,
}


def human_readable_cost_time(cost_time: int) -> str:
    """将耗时转换成人类可读格式

    Args:
        cost_time (int): 耗时，单位为秒

    Returns:
        str: 人类可读格式的耗时字符串
    """
    data = {key: 0 for key in COST_TIME_MAPPING}

    for key, value in COST_TIME_MAPPING.items():
        while value <= cost_time:
            data[key] += 1
            cost_time -= value

    if sum(data.values()) == 0:
        return "0秒"

    if data["分"] == 0 and data["秒"] != 0:
        del data["分"]

    return "".join(f"{value}{key}" for key, value in data.items())
