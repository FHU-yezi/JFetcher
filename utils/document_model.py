from typing import Any, Dict

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


class Field(Struct, **FIELD_OBJECT_CONFIG):
    pass


class Documemt(Struct, **DOCUMENT_OBJECT_CONFIG):
    _id: ObjectId

    def validate(self) -> Self:
        return convert(
            to_builtins(self, builtin_types=(ObjectId,)),
            type=self.__class__,
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Self:
        return convert(data, type=cls)

    def to_dict(self) -> Dict[str, Any]:
        return to_builtins(self, builtin_types=(ObjectId,))
