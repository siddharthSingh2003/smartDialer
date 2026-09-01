import asyncio

import pytest

pytestmark = pytest.mark.asyncio


async def test_claim_is_disjoint_and_bounded(db, repos, campaign_id):
    """Invariant I2 (claim side): a borrower is claimed by at most one worker."""
    async with db.tx() as con:
        await repos.borrowers.seed_many(con, campaign_id, [f"+1555000{i:04d}" for i in range(5)])

    async def attempt(worker: str):
        async with db.tx() as con:
            row = await repos.borrowers.claim_one(con, campaign_id, worker, lease_until=9e9)
            return row["id"] if row else None

    results = await asyncio.gather(*[attempt(f"w{i}") for i in range(20)])
    claimed = [r for r in results if r is not None]
    assert len(claimed) == 5
    assert len(set(claimed)) == 5


async def test_unique_idempotency_key_blocks_double_dial(db, repos, campaign_id):
    """Invariant I2 (call side): UNIQUE(idempotency_key) rejects a re-dial of the
    same borrower at the same attempt number, e.g. after a crash-retry."""
    async with db.tx() as con:
        agent_id = await repos.agents.create(con, campaign_id, "a1")
        await repos.agents.login(con, agent_id)
        await repos.borrowers.seed_many(con, campaign_id, ["+15550000000"])
        borrower = await repos.borrowers.claim_one(con, campaign_id, "w1", 9e9)

    idem = f"{campaign_id}:{borrower['id']}:1"
    async with db.tx() as con:
        await repos.calls.create(con, campaign_id, agent_id, borrower["id"], "mock_a", 1,
                                  idem, "w1", 9e9)

    with pytest.raises(Exception):
        async with db.tx() as con:
            await repos.calls.create(con, campaign_id, agent_id, borrower["id"], "mock_a", 1,
                                      idem, "w1", 9e9)
