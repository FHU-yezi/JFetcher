from datetime import date, datetime
from typing import List

from beanie import Document
from jkit.ranking.daily_update import DailyUpdateRanking, DailyUpdateRankingRecord
from prefect import flow, get_run_logger
from prefect.states import Completed, State
from pydantic import BaseModel, PositiveInt

from utils.db import init_db
from utils.job_model import Job


class UserInfoField(BaseModel):
    slug: str
    name: str


class DailyUpdateRankingRecordModel(Document):
    date: date
    ranking: PositiveInt
    days: PositiveInt
    user_info: UserInfoField

    class Settings:
        name = "daily_update_ranking_records"


def process_item(
    item: DailyUpdateRankingRecord, /, *, current_date: date
) -> DailyUpdateRankingRecordModel:
    return DailyUpdateRankingRecordModel(
        date=current_date,
        ranking=item.ranking,
        days=item.days,
        user_info=UserInfoField(
            slug=item.user_info.slug,
            name=item.user_info.name,
        ),
    )


@flow
async def main() -> State:
    logger = get_run_logger()

    current_date = datetime.now().date()

    await init_db([DailyUpdateRankingRecordModel])
    logger.info("初始化 ODM 模型成功")

    data: List[DailyUpdateRankingRecordModel] = []
    async for item in DailyUpdateRanking():
        processed_item = process_item(item, current_date=current_date)
        data.append(processed_item)

    await DailyUpdateRankingRecordModel.insert_many(data)

    return Completed(message=f"data_count={len(data)}")


fetch_daily_update_ranking_records_job = Job(
    func=main,
    name="采集日更排行榜记录",
    cron="0 1 * * *",
)
