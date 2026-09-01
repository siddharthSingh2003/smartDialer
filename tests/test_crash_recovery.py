import pytest

from smartdialer.domain.enums import AgentState, CallState
from smartdialer.worker.reaper import reap

pytestmark = pytest.mark.asyncio


async def test_crash_mid_dial_then_reaper_reconciles(db, repos, campaign_id, clock):
    """agent RESERVED -> borrower LOCKED -> call INITIATED -> worker killed.
    After the lease expires, the reaper must: fail the call, free the agent,
    reschedule the borrower with a future next_eligible_at and attempt_count=1,
    and a retry of the same attempt must be rejected by UNIQUE(idempotency_key)
    (invariant I8, ARCHITECTURE.md §9 / §12.1)."""
    # The reaper compares lease_expires_at against Postgres' own now(), not
    # against this process's clock — so "the lease expired" is modelled by
    # writing an already-past timestamp, not by advancing a virtual clock
    # nothing is driving forward (VirtualClock.sleep() only resolves once
    # something else pumps it via advance_to_next()/run_until()).
    lease = clock.now() - 1

    async with db.tx() as con:
        agent_id = await repos.agents.create(con, campaign_id, "a1")
        await repos.agents.login(con, agent_id)
        await repos.borrowers.seed_many(con, campaign_id, ["+15550000001"])
        b = await repos.borrowers.claim_one(con, campaign_id, "dead-worker", lease)
        idem = f"{campaign_id}:{b['id']}:1"
        call_id = await repos.calls.create(con, campaign_id, agent_id, b["id"], "mock_a", 1,
                                            idem, "dead-worker", lease)
        await repos.calls.advance(con, call_id, CallState.INITIATED, lease_until=lease)
        await repos.agents.force_state(con, agent_id, AgentState.DIALING,
                                        worker="dead-worker", lease_until=lease, call_id=call_id)

    # simulate the crash: nothing more happens for this call or agent.
    result = await reap(db, repos, clock, registry=None)
    assert result["orphans_failed"] == 1
    assert result["agents_reclaimed"] == 0   # it was DIALING, not RESERVED — released via the call sweep

    async with db.tx() as con:
        call = await con.fetchrow("SELECT * FROM calls WHERE id=$1", call_id)
        agent = await con.fetchrow("SELECT * FROM agents WHERE id=$1", agent_id)
        borrower = await con.fetchrow("SELECT * FROM borrowers WHERE id=$1", b["id"])

    assert call["state"] == "FAILED"
    assert call["failure_reason"] == "LEASE_EXPIRED_NO_EVENTS"
    assert agent["state"] == "AVAILABLE"
    assert borrower["state"] == "PENDING"
    assert borrower["attempt_count"] == 1
    assert borrower["next_eligible_at"] is not None

    # a restart-and-retry with the same attempt number must not double-dial
    with pytest.raises(Exception):
        async with db.tx() as con:
            await repos.calls.create(con, campaign_id, agent_id, b["id"], "mock_a", 1,
                                      idem, "new-worker", lease)


async def test_reserved_only_agent_is_reclaimed_directly(db, repos, campaign_id, clock):
    """A RESERVED (not yet DIALING) agent whose lease expires is reclaimed
    straight back to AVAILABLE — no provider call was ever placed for it."""
    lease = clock.now() - 1
    async with db.tx() as con:
        agent_id = await repos.agents.create(con, campaign_id, "a1")
        await repos.agents.login(con, agent_id)
        row = await con.fetchrow("SELECT id, version FROM agents WHERE id=$1", agent_id)
        await repos.agents.reserve(con, row["id"], row["version"], "dead-worker", lease)

    result = await reap(db, repos, clock, registry=None)
    assert result["agents_reclaimed"] == 1

    async with db.tx() as con:
        agent = await con.fetchrow("SELECT * FROM agents WHERE id=$1", agent_id)
    assert agent["state"] == "AVAILABLE"
