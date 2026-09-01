"""Property test for invariant I1 at the SQL level: across any mix of agent
and worker counts, an agent is reserved by at most one worker, and the number
of successful reservations is exactly min(n_agents, n_workers) — SKIP LOCKED
guarantees every worker either gets a distinct row or legitimately finds none
left, never a collision.
"""
import asyncio
import os

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from smartdialer.db import Db

DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL", os.environ.get("DATABASE_URL",
                                         "postgresql://dialer:dialer@localhost:5432/dialer"))


async def _run_example(n_agents: int, n_workers: int) -> None:
    db = await Db.connect(DATABASE_URL, min_size=2, max_size=min(n_workers + 2, 40))
    try:
        async with db.tx() as con:
            await con.execute(
                "TRUNCATE provider_events, pacing_decisions, calls, borrowers, agents, "
                "campaign_counters, campaigns RESTART IDENTITY CASCADE")
            campaign_id = await con.fetchval(
                "INSERT INTO campaigns (name) VALUES ('prop_reserve') RETURNING id")
            for i in range(n_agents):
                await con.execute(
                    "INSERT INTO agents (campaign_id, ext_ref, state) VALUES ($1,$2,'AVAILABLE')",
                    campaign_id, f"a{i}")

        async def try_reserve_any():
            async with db.tx() as con:
                cands = await con.fetch(
                    "SELECT id, version FROM agents WHERE campaign_id=$1 AND state='AVAILABLE' "
                    "ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED", campaign_id)
                if not cands:
                    return None
                row = await con.fetchrow(
                    "UPDATE agents SET state='RESERVED', version=version+1 "
                    "WHERE id=$1 AND state='AVAILABLE' AND version=$2 RETURNING id",
                    cands[0]["id"], cands[0]["version"])
                return row["id"] if row else None

        results = await asyncio.gather(*[try_reserve_any() for _ in range(n_workers)])
        reserved = [r for r in results if r is not None]

        assert len(reserved) == len(set(reserved))          # I1: never the same agent twice
        assert len(reserved) == min(n_agents, n_workers)
    finally:
        await db.close()


@settings(max_examples=8, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(n_agents=st.integers(1, 15), n_workers=st.integers(1, 30))
def test_never_double_reserve(n_agents: int, n_workers: int):
    asyncio.run(_run_example(n_agents, n_workers))
