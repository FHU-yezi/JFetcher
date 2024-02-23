from typing import List, Optional, Type

from beanie import Document, init_beanie
from motor.motor_asyncio import AsyncIOMotorClient

from models import MODELS as ALL_MODELS
from utils.config import CONFIG

_CLIENT = AsyncIOMotorClient(CONFIG.mongodb.host, CONFIG.mongodb.port)
_DB = _CLIENT[CONFIG.mongodb.database]

RUN_LOG_COLLECTION = _DB.run_log


async def init_db(
    models: Optional[List[Type[Document]]] = None,
) -> None:
    await init_beanie(
        database=_DB,
        document_models=models if models else ALL_MODELS, # type: ignore
    )
