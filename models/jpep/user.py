from datetime import datetime
from typing import Any, Dict, Optional

from jkit.msgspec_constraints import NonNegativeInt, PositiveInt
from sshared.mongo import MODEL_META, Document, Field, Index

from utils.db import JPEP_DB


class CreditHistoryFieldItem(Field, **MODEL_META):
    time: datetime
    value: NonNegativeInt


class UserDocument(Document):
    updated_at: datetime
    id: PositiveInt
    name: str
    hashed_name: Optional[str]
    avatar_url: Optional[str]

    class Meta:  # type: ignore
        collection = JPEP_DB.users
        indexes = (
            Index(keys=("id",), unique=True),
            Index(keys=("updatedAt",)),
        )

    @classmethod
    async def is_record_exist(cls, id: int) -> bool:  # noqa: A002
        return await cls.find_one({"id": id}) is not None

    @classmethod
    async def insert_or_update_one(
        cls,
        *,
        updated_at: Optional[datetime] = None,
        id: int,  # noqa: A002
        name: str,
        hashed_name: str,
        avatar_url: Optional[str],
    ) -> None:
        if not updated_at:
            updated_at = datetime.now()

        if not await cls.is_record_exist(id):
            await cls.insert_one(
                UserDocument(
                    updated_at=updated_at,
                    id=id,
                    name=name,
                    hashed_name=hashed_name,
                    avatar_url=avatar_url,
                )
            )

        db_data = await cls.find_one({"id": id})
        if not db_data:
            raise AssertionError("意外的空值")
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

        await cls.get_collection().update_one({"id": id}, update_data)

    @classmethod
    async def insert_one_if_not_exist(
        cls,
        *,
        updated_at: datetime,
        id: int,  # noqa: A002
        name: str,
        hashed_name: Optional[str],
    ) -> None:
        if not await cls.is_record_exist(id):
            await cls.insert_one(
                UserDocument(
                    updated_at=updated_at,
                    id=id,
                    name=name,
                    hashed_name=hashed_name,
                    avatar_url=None,
                )
            )
