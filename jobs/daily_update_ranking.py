from datetime import date, datetime
from typing import List

from jkit.ranking.daily_update import (
    DailyUpdateRanking,
    DailyUpdateRankingRecord,
)
from prefect import flow
from prefect.states import Completed, State

from models.daily_update_ranking_record import (
    DailyUpdateRankingRecordDocument,
    insert_many,
)
from models.daily_update_ranking_record import (
    init_db as init_daily_update_ranking_record_db,
)
from models.jianshu_user import init_db as init_jianshu_user_db
from models.jianshu_user import insert_or_update_one
from utils.config_generators import (
    generate_deployment_config,
    generate_flow_config,
)


async def process_item(
    item: DailyUpdateRankingRecord, /, *, current_date: date
) -> DailyUpdateRankingRecordDocument:
    await insert_or_update_one(
        slug=item.user_info.slug,
        name=item.user_info.name,
        avatar_url=item.user_info.avatar_url,
    )

    return DailyUpdateRankingRecordDocument(
        date=current_date,
        ranking=item.ranking,
        days=item.days,
        user_slug=item.user_info.slug,
    ).validate()


@flow(
    **generate_flow_config(
        name="采集日更排行榜记录",
    )
)
async def flow_func() -> State:
    await init_daily_update_ranking_record_db()
    await init_jianshu_user_db()

    current_date = datetime.now().date()

    data: List[DailyUpdateRankingRecordDocument] = []
    async for item in DailyUpdateRanking():
        processed_item = await process_item(item, current_date=current_date)
        data.append(processed_item)

    await insert_many(data)

    return Completed(message=f"data_count={len(data)}")


deployment = flow_func.to_deployment(
    **generate_deployment_config(
        name="采集日更排行榜记录",
        cron="0 1 * * *",
    )
)
