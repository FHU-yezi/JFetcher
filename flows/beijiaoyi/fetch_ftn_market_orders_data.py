from collections.abc import AsyncGenerator
from datetime import datetime, timedelta

from jkit.beijiaoyi.ftn_market import FtnMarket, OrderData
from jkit.credentials import BeijiaoyiCredential
from prefect import flow, get_run_logger, task

from models.beijiaoyi.ftn_market_record import FtnMarketRecord
from models.beijiaoyi.ftn_market_summary_record import FtnMarketSummaryRecord
from models.beijiaoyi.ftn_order import FtnOrder, FtnOrdersType
from models.beijiaoyi.user import User
from utils.config import CONFIG
from utils.exceptions import MissingCredentialError
from utils.prefect_helper import get_flow_run_name, get_task_run_name


def get_fetch_time() -> datetime:
    time = datetime.now().replace(second=0, microsecond=0)

    # 12:01 -> timedelta = -1 minute
    if time.minute % 10 < 5:  # noqa: PLR2004
        delta = timedelta(minutes=time.minute % 10)
        return time - delta
    # 11:57 -> timedelta = +3 minute
    else:  # noqa: RET505
        delta = timedelta(minutes=10 - (time.minute % 10))
        return time + delta


@task(task_run_name=get_task_run_name)
async def pre_check() -> None:
    if not CONFIG.beijiaoyi_token:
        raise MissingCredentialError("beijiaoyi_token 未设置")

    # TODO: 实现防重机制


@task(task_run_name=get_task_run_name)
async def iter_ftn_market_orders(
    *,
    type: FtnOrdersType,
) -> AsyncGenerator[OrderData]:
    async for item in FtnMarket(
        credential=BeijiaoyiCredential.from_bearer_token(CONFIG.beijiaoyi_token)
    ).iter_orders(type=type):
        yield item


async def save_user_data(item: OrderData, /) -> None:
    user = await User.get_by_id(item.publisher_info.id)
    if not user:
        await User.create(
            id=item.publisher_info.id,
            name=item.publisher_info.name,
            avatar_url=item.publisher_info.avatar_url,
        )
    else:
        await User.update_by_id(
            id=item.publisher_info.id,
            name=item.publisher_info.name,
            avatar_url=item.publisher_info.avatar_url,
        )


async def save_ftn_order_data(
    item: OrderData, /, *, type: FtnOrdersType, fetch_time: datetime
) -> None:
    ftn_order = await FtnOrder.get_by_id(item.id)
    if not ftn_order:
        await FtnOrder.create(
            id=item.id,
            type=type,
            publisher_id=item.publisher_info.id,
            publish_time=item.publish_time,
            last_seen_time=fetch_time,
        )
    else:
        await FtnOrder.update_by_id(
            id=item.id,
            last_seen_time=fetch_time,
        )


async def save_ftn_market_record_data(
    item: OrderData, /, *, fetch_time: datetime
) -> None:
    await FtnMarketRecord.create(
        fetch_time=fetch_time,
        id=item.id,
        price=item.price,
        total_amount=item.total_amount,
        traded_amount=item.traded_amount,
        remaining_amount=item.remaining_amount,
        minimum_trade_amount=item.minimum_trade_amount,
        maximum_trade_amount=item.maximum_trade_amount,
        completed_trades_count=item.completed_trades_count,
    )


async def save_ftn_market_summary_record_data(
    *, type: FtnOrdersType, fetch_time: datetime
) -> None:
    logger = get_run_logger()

    if not await FtnMarketRecord.exists_by_fetch_time(fetch_time):
        logger.warning("无简书贝市场记录数据，跳过摘要数据写入")
        return

    await FtnMarketSummaryRecord.create_from_ftn_market_records_by_fetch_time_and_type(
        fetch_time=fetch_time, type=type
    )


@flow(
    name="采集贝交易平台简书贝市场订单数据",
    flow_run_name=get_flow_run_name,
    retries=2,
    retry_delay_seconds=10,
    timeout_seconds=20,
)
async def beijiaoyi_fetch_ftn_market_orders_data(type: FtnOrdersType) -> None:
    logger = get_run_logger()

    fetch_time = get_fetch_time()

    await pre_check()

    async for item in iter_ftn_market_orders(type=type):
        try:
            await save_user_data(item)
        except Exception:
            logger.exception("保存用户数据时发生未知异常 id=%s", item.id)

        try:
            await save_ftn_order_data(item, type=type, fetch_time=fetch_time)
        except Exception:
            logger.exception("保存简书贝订单数据时发生未知异常 id=%s", item.id)

        try:
            await save_ftn_market_record_data(item, fetch_time=fetch_time)
        except Exception:
            logger.exception("保存简书贝市场记录数据时发生未知异常 id=%s", item.id)

    try:
        await save_ftn_market_summary_record_data(type=type, fetch_time=fetch_time)
    except Exception:
        logger.exception("保存简书贝市场摘要数据时发生未知异常")
