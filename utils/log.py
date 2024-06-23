from pymongo import MongoClient
from sshared.logging import Logger

from utils.config import CONFIG

logger = Logger(
    # TODO
    save_level=CONFIG.logging.save_level,
    display_level=CONFIG.logging.display_level,
    save_collection=MongoClient()[CONFIG.mongodb.database].log
    if CONFIG.logging.enable_save
    else None,
)
