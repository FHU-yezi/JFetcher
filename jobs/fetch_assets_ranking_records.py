from datetime import date, datetime
from typing import List, Optional

from beanie import Document
from jkit.ranking.assets import AssetsRanking, AssetsRankingRecord
from prefect import flow, get_run_logger
from pydantic import BaseModel, NonNegativeFloat, PositiveFloat, PositiveInt

from utils.db import init_db
from utils.job_model import Job


class UserInfoField(BaseModel):
    id: Optional[PositiveInt]
    slug: Optional[str]
    name: Optional[str]


class AssetsRankingRecordModel(Document):
    date: date
    ranking: PositiveInt

    fp_amount: Optional[NonNegativeFloat]
    ftn_amount: Optional[NonNegativeFloat]
    assets_amount: PositiveFloat

    user_info: UserInfoField

    class Settings:
        name = "assets_ranking_records"
        indexes = ("date", "ranking", "user_info.slug")


async def process_item(
    item: AssetsRankingRecord, /, *, target_date: date
) -> AssetsRankingRecordModel:
    logger = get_run_logger()

    if item.user_info.slug:
        user_obj = item.user_info.to_user_obj()
        fp_amount = await user_obj.fp_amount
        ftn_amount = abs(round(item.assets_amount - fp_amount, 3))
    else:
        logger.warning(f"用户不存在，跳过采集简书钻与简书贝信息 ranking={item.ranking}")
        fp_amount = None
        ftn_amount = None

    return AssetsRankingRecordModel(
        date=target_date,
        ranking=item.ranking,
        fp_amount=fp_amount,
        ftn_amount=ftn_amount,
        assets_amount=item.assets_amount,
        user_info=UserInfoField(
            id=item.user_info.id,
            slug=item.user_info.slug,
            name=item.user_info.name,
        ),
    )


@flow
async def main() -> None:
    logger = get_run_logger()

    await init_db([AssetsRankingRecordModel])
    logger.info("初始化 ODM 模型成功")

    target_date = datetime.now().date()
    logger.info(f"target_date={target_date}")

    data: List[AssetsRankingRecordModel] = []
    async for item in AssetsRanking():
        processed_item = await process_item(item, target_date=target_date)
        data.append(processed_item)

        if len(data) == 1000:
            break

    await AssetsRankingRecordModel.insert_many(data)


fetch_assets_ranking_records_job = Job(
    func=main,
    name="采集资产排行榜记录",
    cron="0 1 * * *",
)
