from datetime import datetime
from typing import Any, ClassVar, Dict, List, Optional

from jkit._constraints import NonNegativeInt, PositiveInt
from pymongo import IndexModel

from utils.db import DB
from utils.document_model import FIELD_OBJECT_CONFIG, Documemt, Field


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

    class Meta:  # type: ignore
        collection = DB.jpep_users
        indexes: ClassVar[List[IndexModel]] = [
            IndexModel(["id"], unique=True),
            IndexModel(["updatedAt"]),
        ]

    @classmethod
    async def is_record_exist(cls, id: int) -> bool:  # noqa: A002
        return await cls.Meta.collection.find_one({"id": id}) is not None

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
            await cls.Meta.collection.find_one({"id": id})  # type: ignore
        )
        # 如果数据库中数据的更新时间晚于本次更新时间，则本次数据已不是最新
        # 此时跳过更新
        if updated_at < db_data.updated_at:
            return

        update_data: Dict[str, Any] = {
            "$set": {
                # 即使没有要更新的数据，也要刷新更新时间
                "updatedAt": updated_at,
            }
        }
        # 如果昵称不一致，更新之
        if (name is not None and db_data.name is not None) and name != db_data.name:
            update_data["$set"]["name"] = name

        # 如果头像链接有变动，更新之
        if avatar_url is not None and avatar_url != db_data.avatar_url:
            update_data["$set"]["avatarUrl"] = avatar_url

        # 如果信用值有变动，将变动信息添加至信用值历史
        if credit != db_data.credit_history[-1].value:
            update_data["$push"]["creditHistory"] = {
                "time": updated_at,
                "value": credit,
            }

        await cls.Meta.collection.update_one({"id": id}, update_data)
