from datetime import datetime, date
from typing import Any, Dict, List


def GetNowWithoutMileseconds() -> datetime:
    return datetime.now().replace(microsecond=0)


def GetTodayInDatetimeObj() -> datetime:
    return datetime.fromisoformat(date.today().strftime(r"%Y-%m-%d"))


def CronToKwargs(cron: str) -> Dict[str, str]:
    second, minute, hour, day, month, day_of_week = cron.split()
    result = {"second": second,
              "minute": minute,
              "hour": hour,
              "day": day,
              "month": month,
              "day_of_week": day_of_week}
    return result


def CostSecondsToString(cost_time: int) -> str:
    # TODO: 这玩意写的一点都不优雅
    mapping = {
        "分": 60,
        "秒": 1
    }
    data = {key: 0 for key in mapping.keys()}

    for key, value in mapping.items():
        while value <= cost_time:
            data[key] += 1
            cost_time -= value

    if sum(data.values()) == 0:
        return "0秒"

    if data["分"] == 0 and data["秒"] != 0:
        del data["分"]

    return "".join(f"{value}{key}" for key, value in data.items())
