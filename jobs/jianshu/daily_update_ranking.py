from datetime import datetime

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
from models.new.jianshu.daily_update_ranking_record import (
    DailyUpdateRankingRecord as NewDbDailyUpateRankingRecord,
)
from utils.log import log_flow_run_start, log_flow_run_success, logger
from utils.prefect_helper import (
    generate_deployment_config,
    generate_flow_config,
)


async def process_item(
    item: DailyUpdateRankingRecord, date: datetime
) -> DailyUpdateRankingRecordDocument:
    await UserDocument.insert_or_update_one(
        slug=item.user_info.slug,
        name=item.user_info.name,
        avatar_url=item.user_info.avatar_url,
    )

    return DailyUpdateRankingRecordDocument(
        date=date,
        ranking=item.ranking,
        days=item.days,
        user_slug=item.user_info.slug,
    )


def transform_to_new_db_model(
    data: list[DailyUpdateRankingRecordDocument],
) -> list[NewDbDailyUpateRankingRecord]:
    result: list[NewDbDailyUpateRankingRecord] = []
    for item in data:
        result.append(  # noqa: PERF401
            NewDbDailyUpateRankingRecord(
                date=item.date.date(),
                ranking=item.ranking,
                slug=item.user_slug,
                days=item.days,
            )
        )

    return result


@flow(
    **generate_flow_config(
        name="采集日更排行榜记录",
    )
)
async def main() -> None:
    log_flow_run_start(logger)

    date = get_today_as_datetime()

    data: list[DailyUpdateRankingRecordDocument] = []
    async for item in DailyUpdateRanking():
        processed_item = await process_item(item, date=date)
        data.append(processed_item)

    await DailyUpdateRankingRecordDocument.insert_many(data)

    new_data = transform_to_new_db_model(data)
    await NewDbDailyUpateRankingRecord.insert_many(new_data)

    log_flow_run_success(logger, data_count=len(data))


deployment = main.to_deployment(
    **generate_deployment_config(
        name="采集日更排行榜记录",
        cron="0 1 * * *",
    )
)
