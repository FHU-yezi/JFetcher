from datetime import datetime
from typing import (
    Any,
    AsyncGenerator,
    ClassVar,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
)

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

_BUILDIN_TYPES: Tuple[object, ...] = (ObjectId, datetime)


class Field(Struct, **FIELD_OBJECT_CONFIG):
    pass


class Document(Struct, **DOCUMENT_OBJECT_CONFIG):
    class Meta(Struct):
        collection: ClassVar[AgnosticCollection]
        indexes: ClassVar[List[IndexModel]]

    @classmethod
    def get_collection(cls) -> AgnosticCollection:
        return cls.Meta.collection

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
        return to_builtins(
            self,
            builtin_types=_BUILDIN_TYPES,
        )

    @classmethod
    async def ensure_indexes(cls) -> None:
        if not cls.Meta.indexes:
            return

        await cls.get_collection().create_indexes(cls.Meta.indexes)

    @classmethod
    async def find_one(
        cls,
        filter: Optional[Dict[str, Any]] = None,  # noqa: A002
        /,
        *,
        sort: Optional[Dict[str, Literal["ASC", "DESC"]]] = None,
    ) -> Optional[Self]:
        cursor = cls.get_collection().find(filter)
        if sort:
            cursor = cursor.sort(
                {key: 1 if order == "ASC" else -1 for key, order in sort.items()}
            )
        cursor = cursor.limit(1)

        try:
            return cls.from_dict(await cursor.__anext__())
        except StopAsyncIteration:
            return None

    @classmethod
    async def find_many(
        cls,
        filter: Optional[Dict[str, Any]] = None,  # noqa: A002
        /,
        *,
        sort: Optional[Dict[str, Literal["ASC", "DESC"]]] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> AsyncGenerator[Self, None]:
        cursor = cls.get_collection().find(filter)
        if sort:
            cursor = cursor.sort(
                {key: 1 if order == "ASC" else -1 for key, order in sort.items()}
            )
        if skip:
            cursor = cursor.skip(skip)
        if limit:
            cursor = cursor.limit(limit)

        async for item in cursor:
            yield cls.from_dict(item)

    @classmethod
    async def insert_one(cls, data: Self) -> None:
        await cls.get_collection().insert_one(data.to_dict())

    @classmethod
    async def insert_many(cls, data: Sequence[Self]) -> None:
        await cls.get_collection().insert_many(x.to_dict() for x in data)

    @classmethod
    async def count(cls, filter: Optional[Dict[str, Any]] = None) -> int:  # noqa: A002
        return await cls.get_collection().count_documents(filter if filter else {})
