from datetime import datetime
from typing import Tuple, Type

from utils.document_model import Document


async def get_collection_data_count(document: Type[Document]) -> int:
    return await document.Meta.collection.count_documents({})


async def get_collection_data_time_range(
    document: Type[Document], key: str
) -> Tuple[datetime, datetime]:
    start_time = (
        await document.Meta.collection.find().sort({key: 1}).limit(1).__anext__()
    )[key]
    end_time = (
        await document.Meta.collection.find().sort({key: -1}).limit(1).__anext__()
    )[key]

    return (start_time, end_time)
