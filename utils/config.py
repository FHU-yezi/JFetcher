from sshared.config import ConfigBase
from sshared.config.blocks import ConfigBlock, LoggingBlock, MongoBlock
from sshared.strict_struct import NonEmptyStr


class FeishuNotificationBlock(ConfigBlock, frozen=True):
    enabled: bool
    webhook_url: NonEmptyStr
    failure_card_id: NonEmptyStr


class _Config(ConfigBase, frozen=True):
    mongodb: MongoBlock
    logging: LoggingBlock
    feishu_notification: FeishuNotificationBlock


CONFIG = _Config.load_from_file("config.toml")
