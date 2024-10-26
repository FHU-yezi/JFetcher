from sshared.postgres import ConnectionManager, enhance_json_process

from utils.config import CONFIG

enhance_json_process()


_jianshu_connection_manager = ConnectionManager(CONFIG.postgres.connection_string)

get_jianshu_conn = _jianshu_connection_manager.get_conn
