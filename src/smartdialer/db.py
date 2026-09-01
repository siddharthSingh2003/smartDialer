from contextlib import asynccontextmanager

import asyncpg


class Db:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    @classmethod
    async def connect(cls, dsn: str, min_size=2, max_size=10) -> "Db":
        return cls(await asyncpg.create_pool(dsn, min_size=min_size, max_size=max_size))

    async def close(self) -> None:
        await self.pool.close()

    @asynccontextmanager
    async def tx(self):
        async with self.pool.acquire() as con, con.transaction():
            yield con

    @asynccontextmanager
    async def dedicated(self):
        """A raw pool connection held for the caller's lifetime, outside pooled
        transactions. Required for session-scoped advisory locks: pg_advisory_lock
        is tied to the *connection*, so leader election must not let the pool hand
        that connection to unrelated work while the lock is held."""
        con = await self.pool.acquire()
        try:
            yield con
        finally:
            await self.pool.release(con)

    async def try_leader(self, con, campaign_id: int) -> bool:
        """Session-scoped advisory lock => at most one pacing loop per campaign.
        Caller must hold `con` from `dedicated()` for as long as it believes it
        is leader, and must call `release_leader` on the same connection when done."""
        return await con.fetchval("SELECT pg_try_advisory_lock($1)", campaign_id)

    async def release_leader(self, con, campaign_id: int) -> None:
        await con.execute("SELECT pg_advisory_unlock($1)", campaign_id)
