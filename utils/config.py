from msgspec import Struct
from sspeedup.config import load_or_save_default_config
from sspeedup.config.blocks import (
    CONFIG_STRUCT_CONFIG,
    LoggingConfig,
    MongoDBConfig,
)


class _Config(Struct, **CONFIG_STRUCT_CONFIG):
    version: str = "v3.0.0"
    mongodb: MongoDBConfig = MongoDBConfig()
    log: LoggingConfig = LoggingConfig()


CONFIG = load_or_save_default_config(_Config)
