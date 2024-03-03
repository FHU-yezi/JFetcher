from typing import List

from jkit.jianshu_lottery import JianshuLottery, JianshuLotteryWinRecord
from prefect import flow, get_run_logger
from prefect.states import Completed, State

from models.jianshu_lottery_win_record import (
    JianshuLotteryWinRecordDocument,
    UserInfoField,
    get_latest_record_id,
    init_db,
    insert_many,
)
from utils.config_generators import (
    generate_deployment_config,
    generate_flow_config,
)


def process_item(item: JianshuLotteryWinRecord, /) -> JianshuLotteryWinRecordDocument:
    return JianshuLotteryWinRecordDocument(
        record_id=item.id,
        time=item.time,
        award_name=item.award_name,
        user_info=UserInfoField(
            id=item.user_info.id,
            slug=item.user_info.slug,
            name=item.user_info.name,
        ),
    ).validate()


@flow(
    **generate_flow_config(
        name="采集简书大转盘抽奖中奖记录",
    )
)
async def flow_func() -> State:
    await init_db()

    logger = get_run_logger()

    stop_id = await get_latest_record_id()
    logger.info(f"获取到最新的记录 ID：{stop_id}")
    if stop_id == 0:
        logger.warning("数据库中没有记录")

    data: List[JianshuLotteryWinRecordDocument] = []
    async for item in JianshuLottery().iter_win_records():
        if item.id == stop_id:
            break

        processed_item = process_item(item)
        data.append(processed_item)
    else:
        logger.warning("采集数据量达到上限")

    if data:
        await insert_many(data)
    else:
        logger.info("无数据，不执行保存操作")

    return Completed(message=f"data_count={len(data)}")


deployment = flow_func.to_deployment(
    **generate_deployment_config(
        name="采集简书大转盘抽奖中奖记录",
        cron="*/10 * * * *",
    )
)
