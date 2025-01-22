from sshared.config import ConfigBase
from sshared.config.blocks import PostgresBlock


class _Config(ConfigBase, frozen=True):
    jianshu_postgres: PostgresBlock
    jpep_postgres: PostgresBlock
    jianshu_endpoint: str


CONFIG = _Config.load_from_file("config.toml")
