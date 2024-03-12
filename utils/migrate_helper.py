from datetime import datetime
from typing import Tuple, Type

from utils.document_model import Document


async def get_collection_data_count(document: Type[Document]) -> int:
    return await document.count()


async def get_collection_data_time_range(
    document: Type[Document], key: str
) -> Tuple[datetime, datetime]:
    start_time = (await document.find_one(sort={key: "ASC"})).__getattribute__(key)
    end_time = (await document.find_one(sort={key: "DESC"})).__getattribute__(key)

    return (start_time, end_time)
