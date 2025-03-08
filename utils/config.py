from sshared.config import ConfigBase
from sshared.config.blocks import PostgresBlock


class _Config(ConfigBase, frozen=True):
    jianshu_endpoint: str
    beijiaoyi_token: str
    jianshu_postgres: PostgresBlock
    jpep_postgres: PostgresBlock
    beijiaoyi_postgres: PostgresBlock


CONFIG = _Config.load_from_file("config.toml")
