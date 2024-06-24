from motor.motor_asyncio import AsyncIOMotorClient

from utils.config import CONFIG

_CLIENT = AsyncIOMotorClient(CONFIG.mongodb.host, CONFIG.mongodb.port)

JIANSHU_DB = _CLIENT.jianshu
JPEP_DB = _CLIENT.jpep
