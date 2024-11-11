from datetime import date

from jkit.ranking.daily_update import (
    DailyUpdateRanking,
    DailyUpdateRankingRecord,
)
from prefect import flow

from models.jianshu.daily_update_ranking_record import (
    DailyUpdateRankingRecord as DbDailyUpdateRankingRecord,
)
from models.jianshu.user import User
from utils.log import log_flow_run_start, log_flow_run_success, logger
from utils.prefect_helper import (
    generate_deployment_config,
    generate_flow_config,
)


async def process_item(
    item: DailyUpdateRankingRecord, date_: date
) -> DbDailyUpdateRankingRecord:
    await User.upsert(
        slug=item.user_info.slug,
        name=item.user_info.name,
        avatar_url=item.user_info.avatar_url,
    )

    return DbDailyUpdateRankingRecord(
        date=date_,
        ranking=item.ranking,
        slug=item.user_info.slug,
        days=item.days,
    )


@flow(
    **generate_flow_config(
        name="采集日更排行榜记录",
    )
)
async def main() -> None:
    log_flow_run_start(logger)

    date_ = date.today()

    data: list[DbDailyUpdateRankingRecord] = []
    async for item in DailyUpdateRanking():
        processed_item = await process_item(item, date_=date_)
        data.append(processed_item)

    await DbDailyUpdateRankingRecord.insert_many(data)

    log_flow_run_success(logger, data_count=len(data))


deployment = main.to_deployment(
    **generate_deployment_config(
        name="采集日更排行榜记录",
        cron="0 1 * * *",
    )
)
