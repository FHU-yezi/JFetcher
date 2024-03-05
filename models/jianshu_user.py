from datetime import datetime
from typing import Any, Dict, List, Optional

from jkit._constraints import PositiveInt, UserName, UserSlug, UserUploadedUrl
from pymongo import IndexModel

from utils.db import DB
from utils.document_model import Documemt

COLLECTION = DB.jianshu_users


class JianshuUser(Documemt):
    slug: UserSlug
    updated_at: datetime
    id: Optional[PositiveInt]
    name: Optional[UserName]
    history_names: List[UserName]
    avatar_url: Optional[UserUploadedUrl]


async def init_db() -> None:
    await COLLECTION.create_indexes(
        [
            IndexModel(["slug"], unique=True),
            IndexModel(["updatedAt"]),
        ],
    )


async def is_exist(slug: str) -> bool:
    return await COLLECTION.find_one({"slug": slug}) is not None


async def insert_or_update_one(
    *,
    slug: str,
    updated_at: Optional[datetime] = None,
    id: Optional[int] = None,  # noqa: A002
    name: Optional[str] = None,
    avatar_url: Optional[str] = None,
) -> None:
    if not updated_at:
        updated_at = datetime.now()

    if not await is_exist(slug):
        await COLLECTION.insert_one(
            JianshuUser(
                slug=slug,
                updated_at=updated_at,
                id=id,
                name=name,
                history_names=[],
                avatar_url=avatar_url,
            ).to_dict()
        )

    # 此处用户必定存在，因此 current_data 不为 None
    current_data = JianshuUser.from_dict(await COLLECTION.find_one({"slug": slug}))  # type: ignore

    update_data: Dict[str, Any] = {
        "$set": {
            # 即使没有要更新的数据，也视为对数据更新时间的刷新
            "updatedAt": updated_at,
        }
    }
    # 如果新数据中的 ID 与数据库不一致，报错
    if id and current_data.id and id != current_data.id:
        raise ValueError(f"ID 不一致（{id} 和 {current_data.id}）")

    # 如果获取到了之前未知的 ID，添加之
    if id and not current_data.id:
        update_data["$set"]["id"] = id

    # 如果获取到了之前未知的昵称，添加之
    if name and not current_data.name:
        update_data["$set"]["name"] = name

    # 如果新数据中的昵称与数据库不一致，说明昵称更改过
    # 更新数据库中的昵称，并将之前的昵称加入历史昵称列表
    if current_data.name is not None:
        update_data["$set"]["name"] = name
        update_data["$push"] = {"historyNames": current_data.name}

    # 如果获取到了之前未知的头像链接，或头像链接与之前不一致，添加 / 更新之
    if avatar_url and (
        not current_data.avatar_url or avatar_url != current_data.avatar_url
    ):
        update_data["$set"]["avatarUrl"] = avatar_url

    await COLLECTION.update_one({"slug": slug}, update_data)
