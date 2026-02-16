import asyncpg
from .config import Settings

async def create_pool(cfg: Settings) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        dsn=cfg.db_dsn,
        min_size=2,
        max_size=20,
        command_timeout=60,
    )
