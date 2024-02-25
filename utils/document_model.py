from datetime import date, datetime
from typing import Any, Dict, Tuple

from bson import ObjectId
from msgspec import Struct, convert, to_builtins
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
    _id: ObjectId

    def validate(self) -> Self:
        return convert(
            to_builtins(self, builtin_types=_BUILDIN_TYPES),
            type=self.__class__,
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Self:
        return convert(data, type=cls)

    def to_dict(self) -> Dict[str, Any]:
        return convert_date_to_datetime(
            to_builtins(
                self,
                builtin_types=_BUILDIN_TYPES,
            )
        )
