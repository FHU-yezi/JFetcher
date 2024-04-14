from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from jkit.msgspec_constraints import PositiveInt, UserName, UserSlug, UserUploadedUrl
from sshared.mongo import MODEL_META, Document, Index

from utils.db import JIANSHU_DB


class JianshuUserStatus(Enum):
    NORMAL = "NORMAL"
    INACCESSABLE = "INACCESSIBLE"


class UserDocument(Document, **MODEL_META):
    slug: UserSlug
    status: JianshuUserStatus
    updated_at: datetime
    id: Optional[PositiveInt]
    name: Optional[UserName]
    history_names: List[UserName]
    avatar_url: Optional[UserUploadedUrl]

    class Meta:  # type: ignore
        collection = JIANSHU_DB.users
        indexes = (
            Index(keys=("slug",), unique=True),
            Index(keys=("updatedAt",)),
        )

    @classmethod
    async def is_record_exist(cls, slug: str) -> bool:
        return await cls.find_one({"slug": slug}) is not None

    @classmethod
    async def insert_or_update_one(
        cls,
        *,
        slug: str,
        updated_at: Optional[datetime] = None,
        id: Optional[int] = None,  # noqa: A002
        name: Optional[str] = None,
        avatar_url: Optional[str] = None,
    ) -> None:
        if not updated_at:
            updated_at = datetime.now()

        if not await cls.is_record_exist(slug):
            await UserDocument(
                slug=slug,
                status=JianshuUserStatus.NORMAL,
                updated_at=updated_at,
                id=id,
                name=name,
                history_names=[],
                avatar_url=avatar_url,
            ).save()
            return

        db_data = await cls.find_one({"slug": slug})
        if not db_data:
            raise AssertionError("意外的空值")
        # 如果数据库中数据的更新时间晚于本次更新时间，则本次数据已不是最新
        # 此时跳过更新
        if updated_at < db_data.updated_at:
            return

        data_to_set: Dict[str, Any] = {
            # 刷新数据更新时间
            "updatedAt": updated_at
        }

        # 如果新数据中的 ID 与数据库不一致，报错
        if (id is not None and db_data.id is not None) and id != db_data.id:
            raise ValueError(f"ID 不一致（{id} 和 {db_data.id}）")

        # 如果获取到了之前未知的 ID，添加之
        if id is not None and db_data.id is None:
            data_to_set["id"] = id

        # 如果获取到了之前未知的昵称，添加之
        if name is not None and db_data.name is None:
            data_to_set["name"] = name

        # 如果昵称有变动，更新之
        if (name is not None and db_data.name is not None) and name != db_data.name:
            data_to_set["name"] = name

            new_history_names = db_data.history_names.copy()

            # 将旧昵称添加到历史昵称列表
            new_history_names.append(db_data.name)
            # 如果新昵称在历史昵称列表中，移除之
            if name in new_history_names:
                new_history_names.remove(name)

            # 如果有变动，将历史昵称列表更新提交至数据库
            if new_history_names != db_data.history_names:
                data_to_set["historyNames"] = new_history_names

        # 如果获取到了之前未知的头像链接，或头像链接有变动，添加 / 更新之
        if avatar_url is not None and (
            db_data.avatar_url is None or avatar_url != db_data.avatar_url
        ):
            data_to_set["avatarUrl"] = avatar_url

        await cls.get_collection().update_one({"slug": slug}, {"$set": data_to_set})
