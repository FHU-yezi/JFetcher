from prefect import serve
from prefect.deployments.runner import RunnerDeployment

from flows.jianshu.fetch_article_earning_ranking_data import (
    jianshu_fetch_article_earning_ranking_data,
)

DEPLOYMENTS: tuple[RunnerDeployment, ...] = (
    jianshu_fetch_article_earning_ranking_data.to_deployment(
        name="JFetcher_采集简书文章收益排行榜数据",
    ), # type: ignore
)

serve(*DEPLOYMENTS)
