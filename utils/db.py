from motor.motor_asyncio import AsyncIOMotorClient

from utils.config import CONFIG

_CLIENT = AsyncIOMotorClient(CONFIG.mongodb.host, CONFIG.mongodb.port)
DB = _CLIENT[CONFIG.mongodb.database]
