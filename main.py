from prefect import serve
from prefect.client.schemas.schedules import CronSchedule
from prefect.deployments.runner import RunnerDeployment

from flows.jianshu.fetch_article_earning_ranking_data import (
    jianshu_fetch_article_earning_ranking_data,
)
from flows.jianshu.fetch_daily_update_ranking_data import (
    jianshu_fetch_daily_update_ranking_data,
)

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
)  # type: ignore

serve(*DEPLOYMENTS)
