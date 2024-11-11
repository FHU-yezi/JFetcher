from utils.db import jianshu_pool, jpep_pool


async def init_db() -> None:
    await jianshu_pool.prepare()
    await jpep_pool.prepare()
