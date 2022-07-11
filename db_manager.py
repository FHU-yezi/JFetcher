from pymongo import MongoClient

from config_manager import config


def InitDB():
    connection: MongoClient = MongoClient(config["db_address"],
                                          config["db_port"])
    db = connection.JFetcherData
    return db


def GetCollection(name: str):
    return db[name]


db = InitDB()

log_db = db.log
