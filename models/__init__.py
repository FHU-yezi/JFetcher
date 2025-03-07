from utils.db import beijiaoyi_pool, jianshu_pool, jpep_pool


async def init_db() -> None:
    await beijiaoyi_pool.prepare()
    await jianshu_pool.prepare()
    await jpep_pool.prepare()
