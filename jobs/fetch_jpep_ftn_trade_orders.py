from datetime import datetime
from typing import List, Optional

from beanie import Document
from jkit.jpep.ftn_macket import FTNMacket, FTNMacketOrderRecord
from prefect import flow, get_run_logger
from pydantic import (
    BaseModel,
    NonNegativeInt,
    PastDatetime,
    PositiveFloat,
    PositiveInt,
)

from utils.db import init_db
from utils.job_model import Job


class PublisherInfoField(BaseModel):
    is_anonymous: bool
    id: Optional[PositiveInt]
    name: Optional[str]
    hashed_name: Optional[str]
    credit: Optional[NonNegativeInt]


class JPEPFTNTradeOrder(Document):
    fetch_time: datetime
    order_id: PositiveInt
    price: PositiveFloat

    total_amount: PositiveInt
    traded_amount: NonNegativeInt
    tradable_amount: NonNegativeInt
    minimum_trade_amount: PositiveInt

    traded_count: NonNegativeInt
    publish_time: PastDatetime

    publisher_info: PublisherInfoField

    class Settings:
        name = "jpep_ftn_trade_orders"
        indexes = ("fetch_time", "order_id")


def get_fetch_time() -> datetime:
    current_dt = datetime.now()

    # 保证采集时间对齐 10 分钟间隔
    return current_dt.replace(minute=current_dt.minute // 10, second=0, microsecond=0)


def process_item(
    item: FTNMacketOrderRecord, /, *, fetch_time: datetime
) -> JPEPFTNTradeOrder:
    return JPEPFTNTradeOrder(
        fetch_time=fetch_time,
        order_id=item.id,
        price=item.price,
        total_amount=item.total_amount,
        traded_amount=item.traded_amount,
        tradable_amount=item.tradable_amount,
        minimum_trade_amount=item.minimum_trade_amount,
        traded_count=item.traded_count,
        publish_time=item.publish_time,
        publisher_info=PublisherInfoField(
            is_anonymous=item.publisher_info.is_anonymous,
            id=item.publisher_info.id,
            name=item.publisher_info.name,
            hashed_name=item.publisher_info.hashed_name,
            credit=item.publisher_info.credit,
        ),
    )


@flow
async def main() -> None:
    logger = get_run_logger()

    fetch_time = get_fetch_time()

    await init_db([JPEPFTNTradeOrder])
    logger.info("初始化 ODM 模型成功")

    buy_data: List[JPEPFTNTradeOrder] = []
    async for item in FTNMacket().iter_orders(type="buy"):
        processed_item = process_item(item, fetch_time=fetch_time)
        buy_data.append(processed_item)

    await JPEPFTNTradeOrder.insert_many(buy_data)
    logger.info(f"采集买单数据成功（{len(buy_data)} 条）")

    sell_data: List[JPEPFTNTradeOrder] = []
    async for item in FTNMacket().iter_orders(type="sell"):
        processed_item = process_item(item, fetch_time=fetch_time)
        sell_data.append(processed_item)

    await JPEPFTNTradeOrder.insert_many(sell_data)
    logger.info(f"采集卖单数据成功（{len(sell_data)} 条）")


fetch_jpep_ftn_trade_orders_job = Job(
    func=main,
    name="采集简书积分兑换平台简书贝交易挂单",
    cron="*/10 * * * *",
)
