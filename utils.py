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


def CostSecondsToString(cost: int) -> str:
    # TODO: 这玩意写的一点都不优雅
    hour, minute, second = 0, 0, 0

    while cost >= 3600:
        hour += 1
        cost -= 3600
    while cost >= 60:
        minute += 1
        cost -= 60
    while cost > 0:
        second += 1
        cost -= 1

    result: List[Any[int, str]] = []
    if hour:
        result.append(hour)
        result.append("小时")
        result.append(minute)
        result.append("分")
        result.append(second)
        result.append("秒")
    elif minute:
        result.append(minute)
        result.append("分")
        result.append(second)
        result.append("秒")
    elif second:
        result.append(second)
        result.append("秒")
    else:
        result.append(0)
        result.append("秒")

    return "".join([str(x) for x in result])
