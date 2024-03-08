from datetime import datetime
from typing import Any, ClassVar, Dict, List, Optional

from jkit._constraints import NonNegativeInt, PositiveInt
from pymongo import IndexModel

from utils.db import DB
from utils.document_model import FIELD_OBJECT_CONFIG, Documemt, Field

COLLECTION = DB.jpep_users


class CreditHistoryFieldItem(Field, **FIELD_OBJECT_CONFIG):
    time: datetime
    value: NonNegativeInt


class JPEPUserDocument(Documemt):
    updated_at: datetime
    id: PositiveInt
    name: str
    hashed_name: str
    avatar_url: Optional[str]
    credit_history: List[CreditHistoryFieldItem]

    class Settings:  # type: ignore
        collection = COLLECTION
        indexes: ClassVar[List[IndexModel]] = [
            IndexModel(["id"], unique=True),
            IndexModel(["updatedAt"]),
        ]

    @classmethod
    async def is_record_exist(cls, id: int) -> bool:  # noqa: A002
        return await COLLECTION.find_one({"id": id}) is not None

    @classmethod
    async def insert_or_update_one(
        cls,
        *,
        updated_at: Optional[datetime] = None,
        id: int,  # noqa: A002
        name: str,
        hashed_name: str,
        avatar_url: Optional[str],
        credit: int,
    ) -> None:
        if not updated_at:
            updated_at = datetime.now()

        if not await cls.is_record_exist(id):
            await cls.insert_one(
                JPEPUserDocument(
                    updated_at=updated_at,
                    id=id,
                    name=name,
                    hashed_name=hashed_name,
                    avatar_url=avatar_url,
                    credit_history=[
                        CreditHistoryFieldItem(
                            time=updated_at,
                            value=credit,
                        ),
                    ],
                )
            )

        # 此处用户必定存在，因此 db_data 不为 None
        db_data = JPEPUserDocument.from_dict(
            await COLLECTION.find_one({"id": id})  # type: ignore
        )

        update_data: Dict[str, Any] = {
            "$set": {
                # 即使没有要更新的数据，也视为对数据更新时间的刷新
                "updatedAt": updated_at,
            }
        }
        # 如果新数据中的昵称与数据库不一致，说明昵称更改过
        # 此时需要比较数据的更新时间，如果数据库中的数据相对较旧
        # 则更新数据库中的昵称
        if (
            (name is not None and db_data.name is not None)
            and name != db_data.name
            and updated_at > db_data.updated_at
        ):
            update_data["$set"]["name"] = name

        # 如果头像链接与之前不一致，更新之
        if avatar_url is not None and avatar_url != db_data.avatar_url:
            update_data["$set"]["avatarUrl"] = avatar_url

        # 如果信用值有变动，更新之
        if credit != db_data.credit_history[-1].value:
            update_data["$push"]["creditHistory"] = {
                "time": updated_at,
                "value": credit,
            }

        await COLLECTION.update_one({"id": id}, update_data)
