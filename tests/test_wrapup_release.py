import pytest

from smartdialer.domain.enums import AgentState
from smartdialer.worker.reaper import reap

pytestmark = pytest.mark.asyncio


async def test_wrapup_agent_is_released_after_its_lease_expires(db, repos, campaign_id, clock):
    """Regression test: WRAP_UP -> AVAILABLE ("wrap timer elapsed" in the
    agent state diagram, ARCHITECTURE.md §4.2) was never implemented —
    nothing ever moved an agent out of WRAP_UP, so every agent got
    permanently stuck after its first completed call. Found by watching a
    live worker run for a few minutes and noticing agent counts never
    recovered. The fix reuses the lease mechanism (like RESERVED/DIALING)
    instead of a wall-clock comparison, so it also works correctly under the
    simulator's VirtualClock. event_applier.py writes this lease as
    clock.now() + wrapup_s when a call COMPLETEs; here we write an
    already-expired one directly, the same trick test_crash_recovery.py
    uses — the reaper compares against Postgres' own now(), not this clock."""
    async with db.tx() as con:
        agent_id = await repos.agents.create(con, campaign_id, "a1")
        await repos.agents.login(con, agent_id)
        await repos.agents.force_state(con, agent_id, AgentState.WRAP_UP,
                                        lease_until=clock.now() - 1)

    async with db.tx() as con:
        agent = await con.fetchrow("SELECT * FROM agents WHERE id=$1", agent_id)
    assert agent["state"] == "WRAP_UP"
    assert agent["lease_expires_at"] is not None

    result = await reap(db, repos, clock, registry=None)
    assert result["agents_wrapup_released"] == 1

    async with db.tx() as con:
        agent = await con.fetchrow("SELECT * FROM agents WHERE id=$1", agent_id)
    assert agent["state"] == "AVAILABLE"
    assert agent["lease_expires_at"] is None


async def test_wrapup_agent_not_released_before_lease_expires(db, repos, campaign_id, clock):
    """The reaper must not release an agent still legitimately wrapping up."""
    async with db.tx() as con:
        agent_id = await repos.agents.create(con, campaign_id, "a1")
        await repos.agents.login(con, agent_id)
        await repos.agents.force_state(con, agent_id, AgentState.WRAP_UP,
                                        lease_until=clock.now() + 3600)

    result = await reap(db, repos, clock, registry=None)
    assert result["agents_wrapup_released"] == 0

    async with db.tx() as con:
        agent = await con.fetchrow("SELECT * FROM agents WHERE id=$1", agent_id)
    assert agent["state"] == "WRAP_UP"
