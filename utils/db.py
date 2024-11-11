from sshared.postgres import Pool, enhance_json_process

from utils.config import CONFIG

enhance_json_process()

jianshu_pool = Pool(
    CONFIG.jianshu_postgres.connection_string,
    min_size=1,
    max_size=4,
    app_name="JFetcher",
)
jpep_pool = Pool(
    CONFIG.jianshu_postgres.connection_string,
    min_size=1,
    max_size=4,
    app_name="JFetcher",
)
