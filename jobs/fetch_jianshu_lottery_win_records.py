from datetime import datetime
from typing import List

from jkit._constraints import PositiveInt
from jkit.jianshu_lottery import JianshuLottery, JianshuLotteryWinRecord
from prefect import flow, get_run_logger
from prefect.states import Completed, State
from pymongo import DESCENDING

from utils.config_generators import generate_deployment_config, generate_flow_config
from utils.db import DB
from utils.document_model import (
    DOCUMENT_OBJECT_CONFIG,
    FIELD_OBJECT_CONFIG,
    Documemt,
    Field,
)

COLLECTION = DB.jianshu_lottery_win_records


class UserInfoField(Field, **FIELD_OBJECT_CONFIG):
    id: PositiveInt
    slug: str
    name: str


class JianshuLotteryWinRecordDocument(Documemt, **DOCUMENT_OBJECT_CONFIG):
    _id: PositiveInt  # type: ignore
    time: datetime
    award_name: str
    user_info: UserInfoField


async def get_latest_stored_record_id() -> int:
    try:
        latest_data = JianshuLotteryWinRecordDocument.from_dict(
            await COLLECTION.find().sort("_id", DESCENDING).__anext__()
        )
    except StopAsyncIteration:
        return 0

    return latest_data._id


def process_item(item: JianshuLotteryWinRecord, /) -> JianshuLotteryWinRecordDocument:
    return JianshuLotteryWinRecordDocument(
        _id=item.id,
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
    logger = get_run_logger()

    stop_id = await get_latest_stored_record_id()
    logger.info(f"获取到最新的已存储 ID：{stop_id}")
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
        await COLLECTION.insert_many(x.to_dict() for x in data)
    else:
        logger.info("无数据，不执行保存操作")

    return Completed(message=f"data_count={len(data)}")


deployment = flow_func.to_deployment(
    **generate_deployment_config(
        name="采集简书大转盘抽奖中奖记录",
        cron="*/10 * * * *",
    )
)
