from sshared.config import ConfigBase
from sshared.config.blocks import GotifyBlock, LoggingBlock, MongoBlock, PostgresBlock


class _Config(ConfigBase, frozen=True):
    mongo: MongoBlock
    jianshu_postgres: PostgresBlock
    jpep_postgres: PostgresBlock
    logging: LoggingBlock
    notify: GotifyBlock


CONFIG = _Config.load_from_file("config.toml")
