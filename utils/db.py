from typing import List, Optional, Type

from beanie import Document, init_beanie
from motor.motor_asyncio import AsyncIOMotorClient

from utils.config import CONFIG

_CLIENT = AsyncIOMotorClient(CONFIG.mongodb.host, CONFIG.mongodb.port)
_DB = _CLIENT[CONFIG.mongodb.database]


async def init_db(models: Optional[List[Type[Document]]]) -> None:
    await init_beanie(database=_DB, document_models=models)  # type: ignore
