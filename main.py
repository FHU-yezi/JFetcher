from prefect import serve
from prefect.client.schemas.schedules import CronSchedule
from prefect.deployments.runner import RunnerDeployment

from flows.jianshu.fetch_article_earning_ranking_data import (
    jianshu_fetch_article_earning_ranking_data,
)
from flows.jianshu.fetch_daily_update_ranking_data import (
    jianshu_fetch_daily_update_ranking_data,
)
from flows.jianshu.fetch_user_assets_ranking_data import (
    jianshu_fetch_user_assets_ranking_data,
)
from flows.jianshu.fetch_user_earning_ranking_data import (
    jianshu_fetch_user_earning_ranking_data,
)
from flows.jpep.fetch_ftn_market_orders_data import jpep_ftn_ftn_market_orders_data

DEPLOYMENTS: tuple[RunnerDeployment, ...] = (
    jianshu_fetch_article_earning_ranking_data.to_deployment(
        name="JFetcher_采集简书文章收益排行榜数据",
        schedules=(
            CronSchedule(
                cron="45 0 * * *",
                timezone="Asia/Shanghai",
            ),
        ),
    ),
    jianshu_fetch_daily_update_ranking_data.to_deployment(
        name="JFetcher_采集简书日更排行榜数据",
        schedules=(
            CronSchedule(
                cron="0 3 * * *",
                timezone="Asia/Shanghai",
            ),
        ),
    ),
    jianshu_fetch_user_assets_ranking_data.to_deployment(
        name="JFetcher_采集简书用户资产排行榜数据",
        schedules=(
            CronSchedule(
                cron="0 1 * * *",
                timezone="Asia/Shanghai",
            ),
        ),
    ),
    jianshu_fetch_user_earning_ranking_data.to_deployment(
        name="JFetcher_采集简书用户全部收益排行榜数据",
        parameters={"type": "ALL"},
        schedules=(
            CronSchedule(
                cron="50 0 * * *",
                timezone="Asia/Shanghai",
            ),
        ),
    ),
    jianshu_fetch_user_earning_ranking_data.to_deployment(
        name="JFetcher_采集简书用户创作收益排行榜数据",
        parameters={"type": "CREATING"},
        schedules=(
            CronSchedule(
                cron="50 0 * * *",
                timezone="Asia/Shanghai",
            ),
        ),
    ),
    jianshu_fetch_user_earning_ranking_data.to_deployment(
        name="JFetcher_采集简书用户投票收益排行榜数据",
        parameters={"type": "VOTING"},
        schedules=(
            CronSchedule(
                cron="50 0 * * *",
                timezone="Asia/Shanghai",
            ),
        ),
    ),
    jpep_ftn_ftn_market_orders_data.to_deployment(
        name="JFetcher_采集简书积分兑换平台简书贝市场买单挂单数据",
        parameters={"type": "BUY"},
        schedules=(
            CronSchedule(
                cron="*/10 * * * *",
                timezone="Asia/Shanghai",
            ),
        ),
    ),
    jpep_ftn_ftn_market_orders_data.to_deployment(
        name="JFetcher_采集简书积分兑换平台简书贝市场卖单挂单数据",
        parameters={"type": "SELL"},
        schedules=(
            CronSchedule(
                cron="*/10 * * * *",
                timezone="Asia/Shanghai",
            ),
        ),
    ),
)  # type: ignore

serve(*DEPLOYMENTS)
