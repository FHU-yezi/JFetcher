from datetime import date, datetime
from typing import List

from bson import ObjectId
from jkit._constraints import PositiveInt
from jkit.ranking.daily_update import DailyUpdateRanking, DailyUpdateRankingRecord
from prefect import flow
from prefect.states import Completed, State

from utils.config_generators import generate_deployment_config, generate_flow_config
from utils.db import DB
from utils.document_model import (
    DOCUMENT_OBJECT_CONFIG,
    FIELD_OBJECT_CONFIG,
    Documemt,
    Field,
)

COLLECTION = DB.daily_update_ranking_records


class UserInfoField(Field, **FIELD_OBJECT_CONFIG):
    slug: str
    name: str


class DailyUpdateRankingRecordDocument(Documemt, **DOCUMENT_OBJECT_CONFIG):
    date: date
    ranking: PositiveInt
    days: PositiveInt
    user_info: UserInfoField


def process_item(
    item: DailyUpdateRankingRecord, /, *, current_date: date
) -> DailyUpdateRankingRecordDocument:
    return DailyUpdateRankingRecordDocument(
        _id=ObjectId(),
        date=current_date,
        ranking=item.ranking,
        days=item.days,
        user_info=UserInfoField(
            slug=item.user_info.slug,
            name=item.user_info.name,
        ),
    )


@flow(
    **generate_flow_config(
        name="采集日更排行榜记录",
    )
)
async def flow_func() -> State:
    current_date = datetime.now().date()

    data: List[DailyUpdateRankingRecordDocument] = []
    async for item in DailyUpdateRanking():
        processed_item = process_item(item, current_date=current_date)
        data.append(processed_item)

    await COLLECTION.insert_many(x.to_dict() for x in data)

    return Completed(message=f"data_count={len(data)}")


deployment = flow_func.to_deployment(
    **generate_deployment_config(
        name="采集日更排行榜记录",
        cron="0 1 * * *",
    )
)
