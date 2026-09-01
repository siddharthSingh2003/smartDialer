import asyncio

import pytest

pytestmark = pytest.mark.asyncio


async def test_only_one_worker_reserves(db, repos, campaign_id):
    """Invariant I1: an agent is reserved by at most one worker at any instant."""
    async with db.tx() as con:
        agent_id = await repos.agents.create(con, campaign_id, "a1")
        await repos.agents.login(con, agent_id)

    async def attempt(worker: str) -> bool:
        async with db.tx() as con:
            row = await con.fetchrow("SELECT id, version FROM agents WHERE id=$1", agent_id)
            return await repos.agents.reserve(con, row["id"], row["version"], worker,
                                               lease_until=9e9)

    results = await asyncio.gather(*[attempt(f"w{i}") for i in range(50)])
    assert sum(results) == 1

    async with db.tx() as con:
        row = await con.fetchrow("SELECT state FROM agents WHERE id=$1", agent_id)
    assert row["state"] == "RESERVED"


async def test_pick_candidates_gives_disjoint_sets_under_skip_locked(db, repos, campaign_id):
    async with db.tx() as con:
        for i in range(10):
            aid = await repos.agents.create(con, campaign_id, f"a{i}")
            await repos.agents.login(con, aid)

    async def worker(name: str):
        async with db.tx() as con:
            cands = await repos.agents.pick_candidates(con, campaign_id, 3)
            await asyncio.sleep(0.05)   # hold the row lock briefly to force real contention
            for c in cands:
                await repos.agents.reserve(con, c["id"], c["version"], name, lease_until=9e9)
            return [c["id"] for c in cands]

    results = await asyncio.gather(*[worker(f"w{i}") for i in range(4)])
    flat = [i for r in results for i in r]
    assert len(flat) == len(set(flat))          # I1 holds across the whole batch too
