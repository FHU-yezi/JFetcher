from datetime import datetime
from typing import Optional

from jkit.config import CONFIG
from jkit.exceptions import ResourceUnavailableError
from jkit.ranking.assets import AssetsRanking, AssetsRankingRecord
from jkit.user import User
from prefect import flow
from sshared.retry.asyncio import retry
from sshared.time import get_today_as_datetime

from models.jianshu.assets_ranking_record import (
    AmountField,
    AssetsRankingRecordDocument,
)
from models.jianshu.user import UserDocument
from models.new.jianshu.user import User as NewDbUser
from models.new.jianshu.user_assets_ranking_record import (
    UserAssetsRankingRecord as NewDbAssetsRankingRecord,
)
from utils.log import (
    get_flow_run_name,
    log_flow_run_start,
    log_flow_run_success,
    logger,
)
from utils.prefect_helper import (
    generate_deployment_config,
    generate_flow_config,
)


@retry(attempts=5, delay=10)
async def get_fp_ftn_amount(
    item: AssetsRankingRecord, /
) -> tuple[Optional[float], Optional[float]]:
    flow_run_name = get_flow_run_name()

    if not item.user_info.slug:
        logger.warn(
            "用户状态异常，跳过采集简书钻与简书贝信息",
            flow_run_name=flow_run_name,
            ranking=item.ranking,
        )
        return None, None

    try:
        # TODO: 临时解决简书系统问题数据负数导致的报错
        CONFIG.data_validation.enabled = False
        # 此处使用 Slug 初始化用户对象，以对其进行可用性检查
        user_obj = User.from_slug(item.user_info.slug)
        fp_amount = await user_obj.fp_amount
        ftn_amount = abs(round(item.assets_amount - fp_amount, 3))
        CONFIG.data_validation.enabled = True

        return fp_amount, ftn_amount
    except ResourceUnavailableError:
        logger.warn(
            "用户状态异常，跳过采集简书钻与简书贝信息",
            flow_run_name=flow_run_name,
            ranking=item.ranking,
        )
        return None, None


async def process_item(
    item: AssetsRankingRecord, date: datetime
) -> AssetsRankingRecordDocument:
    fp_amount, ftn_amount = await get_fp_ftn_amount(item)

    if item.user_info.slug:
        await UserDocument.insert_or_update_one(
            slug=item.user_info.slug,
            id=item.user_info.id,
            name=item.user_info.name,
            avatar_url=item.user_info.avatar_url,
        )
        await NewDbUser.upsert(
            slug=item.user_info.slug,
            id=item.user_info.id,
            name=item.user_info.name,
            avatar_url=item.user_info.avatar_url,
        )

    return AssetsRankingRecordDocument(
        date=date,
        ranking=item.ranking,
        amount=AmountField(
            fp=fp_amount,
            ftn=ftn_amount,
            assets=item.assets_amount,
        ),
        user_slug=item.user_info.slug,
    )


def transform_to_new_db_model(
    data: list[AssetsRankingRecordDocument],
) -> list[NewDbAssetsRankingRecord]:
    result: list[NewDbAssetsRankingRecord] = []
    for item in data:
        result.append(  # noqa: PERF401
            NewDbAssetsRankingRecord(
                date=item.date.date(),
                ranking=item.ranking,
                slug=item.user_slug,
                fp=item.amount.fp,
                ftn=item.amount.ftn,
                assets=item.amount.assets,
            )
        )

    return result


@flow(
    **generate_flow_config(
        name="采集用户资产排行榜记录",
    )
)
async def main() -> None:
    log_flow_run_start(logger)

    date = get_today_as_datetime()

    data: list[AssetsRankingRecordDocument] = []
    async for item in AssetsRanking():
        processed_item = await process_item(item, date=date)
        data.append(processed_item)

        if len(data) == 1000:
            break

    await AssetsRankingRecordDocument.insert_many(data)

    new_data = transform_to_new_db_model(data)
    await NewDbAssetsRankingRecord.insert_many(new_data)

    log_flow_run_success(logger, data_count=len(data))


deployment = main.to_deployment(
    **generate_deployment_config(
        name="采集用户资产排行榜记录",
        cron="0 1 * * *",
    )
)
