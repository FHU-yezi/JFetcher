from datetime import date, datetime
from typing import List, Optional

from bson import ObjectId
from jkit._constraints import NonNegativeFloat, PositiveFloat, PositiveInt
from jkit.config import CONFIG
from jkit.exceptions import ResourceUnavailableError
from jkit.ranking.assets import AssetsRanking, AssetsRankingRecord
from prefect import flow, get_run_logger
from prefect.states import Completed, State

from utils.db import DB
from utils.document_model import (
    DOCUMENT_OBJECT_CONFIG,
    FIELD_OBJECT_CONFIG,
    Documemt,
    Field,
)
from utils.job_model import Job

COLLECTION = DB.assets_ranking_records


class UserInfoField(Field, **FIELD_OBJECT_CONFIG):
    id: Optional[PositiveInt]
    slug: Optional[str]
    name: Optional[str]


class AssetsRankingRecordDocument(Documemt, **DOCUMENT_OBJECT_CONFIG):
    date: date
    ranking: PositiveInt

    fp_amount: Optional[NonNegativeFloat]
    ftn_amount: Optional[NonNegativeFloat]
    assets_amount: PositiveFloat

    user_info: UserInfoField


async def process_item(
    item: AssetsRankingRecord, /, *, target_date: date
) -> AssetsRankingRecordDocument:
    logger = get_run_logger()

    if item.user_info.slug:
        user_obj = item.user_info.to_user_obj()
        try:
            # TODO: 强制重新检查
            # TODO: 临时解决简书系统问题数据负数导致的报错
            CONFIG.resource_check.force_check_safe_data = True
            CONFIG.data_validation.enabled = False
            fp_amount = await user_obj.fp_amount
            ftn_amount = abs(round(item.assets_amount - fp_amount, 3))
            CONFIG.data_validation.enabled = True
            CONFIG.resource_check.force_check_safe_data = False
        except ResourceUnavailableError:
            logger.warning(
                f"用户已注销或被封禁，跳过采集简书钻与简书贝信息 ranking={item.ranking}"
            )
            fp_amount = None
            ftn_amount = None

    else:
        logger.warning(f"用户不存在，跳过采集简书钻与简书贝信息 ranking={item.ranking}")
        fp_amount = None
        ftn_amount = None

    return AssetsRankingRecordDocument(
        _id=ObjectId(),
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
async def main() -> State:
    target_date = datetime.now().date()

    data: List[AssetsRankingRecordDocument] = []
    async for item in AssetsRanking():
        processed_item = await process_item(item, target_date=target_date)
        data.append(processed_item)

        if len(data) == 1000:
            break

    await COLLECTION.insert_many(x.to_dict() for x in data)

    return Completed(message=f"target_date={target_date}, data_count={len(data)}")


fetch_assets_ranking_records_job = Job(
    func=main,
    name="采集资产排行榜记录",
    cron="0 1 * * *",
)
