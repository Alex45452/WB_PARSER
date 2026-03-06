import asyncpg
from typing import Any, Iterable

class DB:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self.pool: asyncpg.Pool | None = None

    async def open(self) -> None:
        self.pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=10)

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()

    async def fetch(self, sql: str, *args) -> list[asyncpg.Record]:
        assert self.pool
        async with self.pool.acquire() as con:
            return await con.fetch(sql, *args)

    async def fetchrow(self, sql: str, *args) -> asyncpg.Record | None:
        assert self.pool
        async with self.pool.acquire() as con:
            return await con.fetchrow(sql, *args)

    async def execute(self, sql: str, *args) -> str:
        assert self.pool
        async with self.pool.acquire() as con:
            return await con.execute(sql, *args)

    async def executemany(self, sql: str, args_iter: Iterable[tuple[Any, ...]]) -> None:
        assert self.pool
        async with self.pool.acquire() as con:
            await con.executemany(sql, list(args_iter))