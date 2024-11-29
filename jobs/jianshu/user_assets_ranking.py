from __future__ import annotations

from datetime import date

from jkit.config import CONFIG
from jkit.exceptions import ResourceUnavailableError
from jkit.ranking.assets import AssetsRanking, AssetsRankingRecord
from jkit.user import User
from prefect import flow
from sshared.retry.asyncio import retry

from models.jianshu.user import User as DbUser
from models.jianshu.user_assets_ranking_record import (
    UserAssetsRankingRecord as DbAssetsRankingRecord,
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
) -> tuple[float | None, float | None]:
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
    except ResourceUnavailableError:
        logger.warn(
            "用户状态异常，跳过采集简书钻与简书贝信息",
            flow_run_name=flow_run_name,
            ranking=item.ranking,
        )
        return None, None
    else:
        return fp_amount, ftn_amount


async def process_item(item: AssetsRankingRecord, date_: date) -> DbAssetsRankingRecord:
    fp_amount, ftn_amount = await get_fp_ftn_amount(item)

    if item.user_info.slug:
        await DbUser.upsert(
            slug=item.user_info.slug,
            id=item.user_info.id,
            name=item.user_info.name,
            avatar_url=item.user_info.avatar_url,
        )

    return DbAssetsRankingRecord(
        date=date_,
        ranking=item.ranking,
        slug=item.user_info.slug,
        fp=fp_amount,
        ftn=ftn_amount,
        assets=item.assets_amount,
    )


@flow(
    **generate_flow_config(
        name="采集用户资产排行榜记录",
    )
)
async def main() -> None:
    log_flow_run_start(logger)

    date_ = date.today()

    data: list[DbAssetsRankingRecord] = []
    async for item in AssetsRanking():
        processed_item = await process_item(item, date_=date_)
        data.append(processed_item)

        # TODO
        if len(data) == 1000:  # noqa: PLR2004
            break

    await DbAssetsRankingRecord.insert_many(data)

    log_flow_run_success(logger, data_count=len(data))


deployment = main.to_deployment(
    **generate_deployment_config(
        name="采集用户资产排行榜记录",
        cron="0 1 * * *",
    )
)
