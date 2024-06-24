from typing import List

from jkit.lottery import Lottery, LotteryWinRecord
from prefect import flow

from models.jianshu.lottery_win_record import (
    LotteryWinRecordDocument,
)
from models.jianshu.user import UserDocument
from utils.config_generators import (
    generate_deployment_config,
    generate_flow_config,
)
from utils.log import log_flow_run_start, log_flow_run_success, logger


async def process_item(item: LotteryWinRecord, /) -> LotteryWinRecordDocument:
    await UserDocument.insert_or_update_one(
        slug=item.user_info.slug,
        updated_at=item.time,
        id=item.user_info.id,
        name=item.user_info.name,
        avatar_url=item.user_info.avatar_url,
    )

    return LotteryWinRecordDocument(
        id=item.id,
        time=item.time,
        award_name=item.award_name,
        user_slug=item.user_info.slug,
    ).validate()


@flow(
    **generate_flow_config(
        name="采集简书大转盘抽奖中奖记录",
    )
)
async def flow_func() -> None:
    flow_run_name = log_flow_run_start(logger)

    stop_id = await LotteryWinRecordDocument.get_latest_record_id()
    logger.info(f"获取到最新的记录 ID：{stop_id}", flow_run_name=flow_run_name)
    if stop_id == 0:
        logger.warn("数据库中没有记录", flow_run_name=flow_run_name)

    data: List[LotteryWinRecordDocument] = []
    async for item in Lottery().iter_win_records():
        if item.id == stop_id:
            break

        processed_item = await process_item(item)
        data.append(processed_item)
    else:
        logger.warn("采集数据量达到上限", flow_run_name=flow_run_name)

    if data:
        await LotteryWinRecordDocument.insert_many(data)
    else:
        logger.info("无数据，不执行保存操作")

    log_flow_run_success(logger, data_count=len(data))


deployment = flow_func.to_deployment(
    **generate_deployment_config(
        name="采集简书大转盘抽奖中奖记录",
        cron="*/10 * * * *",
    )
)
