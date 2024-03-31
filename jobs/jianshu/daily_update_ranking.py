from datetime import datetime
from typing import List

from jkit.ranking.daily_update import (
    DailyUpdateRanking,
    DailyUpdateRankingRecord,
)
from prefect import flow
from prefect.states import Completed, State
from sshared.time import get_today_as_datetime

from models.jianshu.daily_update_ranking_record import (
    DailyUpdateRankingRecordDocument,
)
from models.jianshu.user import JianshuUserDocument
from utils.config_generators import (
    generate_deployment_config,
    generate_flow_config,
)


async def process_item(
    item: DailyUpdateRankingRecord, /, *, current_date: datetime
) -> DailyUpdateRankingRecordDocument:
    await JianshuUserDocument.insert_or_update_one(
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
    await DailyUpdateRankingRecordDocument.ensure_indexes()
    await JianshuUserDocument.ensure_indexes()

    current_date = get_today_as_datetime()

    data: List[DailyUpdateRankingRecordDocument] = []
    async for item in DailyUpdateRanking():
        processed_item = await process_item(item, current_date=current_date)
        data.append(processed_item)

    await DailyUpdateRankingRecordDocument.insert_many(data)

    return Completed(message=f"data_count={len(data)}")


deployment = flow_func.to_deployment(
    **generate_deployment_config(
        name="采集日更排行榜记录",
        cron="0 1 * * *",
    )
)
