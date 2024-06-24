from datetime import datetime
from typing import List

from jkit.ranking.daily_update import (
    DailyUpdateRanking,
    DailyUpdateRankingRecord,
)
from prefect import flow
from sshared.time import get_today_as_datetime

from models.jianshu.daily_update_ranking_record import (
    DailyUpdateRankingRecordDocument,
)
from models.jianshu.user import UserDocument
from utils.log import log_flow_run_start, log_flow_run_success, logger
from utils.prefect_helper import (
    generate_deployment_config,
    generate_flow_config,
)


async def process_item(
    item: DailyUpdateRankingRecord, /, *, current_date: datetime
) -> DailyUpdateRankingRecordDocument:
    await UserDocument.insert_or_update_one(
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
async def main() -> None:
    log_flow_run_start(logger)

    current_date = get_today_as_datetime()

    data: List[DailyUpdateRankingRecordDocument] = []
    async for item in DailyUpdateRanking():
        processed_item = await process_item(item, current_date=current_date)
        data.append(processed_item)

    await DailyUpdateRankingRecordDocument.insert_many(data)

    log_flow_run_success(logger, data_count=len(data))


deployment = main.to_deployment(
    **generate_deployment_config(
        name="采集日更排行榜记录",
        cron="0 1 * * *",
    )
)
