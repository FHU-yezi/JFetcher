from datetime import date, datetime
from typing import Any, ClassVar, Dict, List, Sequence, Tuple

from bson import ObjectId
from motor.core import AgnosticCollection
from msgspec import Struct, convert, to_builtins
from pymongo import IndexModel
from typing_extensions import Self

FIELD_OBJECT_CONFIG = {
    "kw_only": True,
    "rename": "camel",
}

DOCUMENT_OBJECT_CONFIG = {
    "kw_only": True,
    "rename": "camel",
}

_BUILDIN_TYPES: Tuple[object, ...] = (ObjectId, datetime, date)


def convert_date_to_datetime(data: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in data.items():
        if isinstance(value, date):
            data[key] = datetime.fromisoformat(value.isoformat())

        if isinstance(value, dict):
            data[key] = convert_date_to_datetime(value)

    return data


class Field(Struct, **FIELD_OBJECT_CONFIG):
    pass


class Documemt(Struct, **DOCUMENT_OBJECT_CONFIG):
    class Meta(Struct):
        collection: ClassVar[AgnosticCollection]
        indexes: ClassVar[List[IndexModel]]

    def validate(self) -> Self:
        return convert(
            to_builtins(self, builtin_types=_BUILDIN_TYPES),
            type=self.__class__,
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Self:
        if not hasattr(cls, "_id") and "_id" in data:
            del data["_id"]

        return convert(data, type=cls)

    def to_dict(self) -> Dict[str, Any]:
        return convert_date_to_datetime(
            to_builtins(
                self,
                builtin_types=_BUILDIN_TYPES,
            )
        )

    @classmethod
    async def ensure_indexes(cls) -> None:
        if not cls.Meta.indexes:
            return

        await cls.Meta.collection.create_indexes(cls.Meta.indexes)

    @classmethod
    async def insert_one(cls, data: Self) -> None:
        await cls.Meta.collection.insert_one(data.to_dict())

    @classmethod
    async def insert_many(cls, data: Sequence[Self]) -> None:
        await cls.Meta.collection.insert_many(x.to_dict() for x in data)
