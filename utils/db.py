from pymongo import MongoClient
from pymongo.collection import Collection

from utils.config import config


def init_DB(db_name: str):  # noqa
    connection: MongoClient = MongoClient(config.db.host, config.db.port)
    return connection[db_name]


def get_collection(collection_name: str) -> Collection:
    return db[collection_name]


db = init_DB(config.db.main_database)
run_log_db = db.log
