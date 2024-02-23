from typing import List

from beanie import Document
from jkit.jianshu_lottery import JianshuLottery, JianshuLotteryWinRecord
from prefect import flow, get_run_logger
from pydantic import BaseModel, PastDatetime, PositiveInt

from utils.db import init_db
from utils.job_model import Job


class UserInfoField(BaseModel):
    id: PositiveInt
    slug: str
    name: str


class JianshuLotteryWinRecordModel(Document):
    record_id: PositiveInt
    time: PastDatetime
    award_name: str
    user_info: UserInfoField

    class Settings:
        name = "jianshu_lottery_win_records"
        indexes = ("record_id", "time", "award_name", "user.slug", "user.name")


async def get_latest_stored_record_id() -> int:
    latest_data = (
        await JianshuLotteryWinRecordModel.find().sort("-record_id").first_or_none()
    )
    if not latest_data:
        return 0

    return latest_data.record_id


def process_item(item: JianshuLotteryWinRecord, /) -> JianshuLotteryWinRecordModel:
    return JianshuLotteryWinRecordModel(
        record_id=item.id,
        time=item.time,
        award_name=item.award_name,
        user_info=UserInfoField(
            id=item.user_info.id,
            slug=item.user_info.slug,
            name=item.user_info.name,
        ),
    )


@flow
async def main() -> None:
    logger = get_run_logger()

    await init_db([JianshuLotteryWinRecordModel])
    logger.info("初始化 ODM 模型成功")

    stop_id = await get_latest_stored_record_id()
    logger.info(f"获取到最新的已存储 ID：{stop_id}")
    if stop_id == 0:
        logger.warning("数据库中没有记录")

    data: List[JianshuLotteryWinRecordModel] = []
    async for item in JianshuLottery().iter_win_records():
        if item.id == stop_id:
            break

        processed_item = process_item(item)
        data.append(processed_item)
    else:
        logger.warning("采集数据量达到上限")

    logger.info(f"采集数据量：{len(data)}")

    if data:
        await JianshuLotteryWinRecordModel.insert_many(data)
    else:
        logger.info("无数据，不执行保存操作")


fetch_jianshu_lottery_win_records_job = Job(
    func=main,
    name="采集简书大转盘抽奖中奖记录",
    cron="*/10 * * * *",
)
