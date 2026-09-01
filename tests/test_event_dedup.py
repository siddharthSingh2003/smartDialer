import pytest

from smartdialer.clock import RealClock
from smartdialer.domain.enums import AgentState, CallState
from smartdialer.worker.event_applier import apply_one

pytestmark = pytest.mark.asyncio


async def _make_initiated_call(db, repos, campaign_id, pcid="PCID-1"):
    async with db.tx() as con:
        agent_id = await repos.agents.create(con, campaign_id, "a1")
        await repos.agents.login(con, agent_id)
        await repos.borrowers.seed_many(con, campaign_id, ["+15551234567"])
        b = await repos.borrowers.claim_one(con, campaign_id, "w1", 9e9)
        call_id = await repos.calls.create(con, campaign_id, agent_id, b["id"], "mock_a", 1,
                                            f"{campaign_id}:{b['id']}:1", "w1", 9e9)
        await repos.calls.advance(con, call_id, CallState.INITIATED,
                                   provider_call_id=pcid, lease_until=9e9)
        await repos.agents.force_state(con, agent_id, AgentState.DIALING,
                                        worker="w1", lease_until=9e9, call_id=call_id)
    return call_id, agent_id, b["id"]


async def _ingest_and_apply(db, repos, event_id, pcid, etype):
    async with db.tx() as con:
        row_id = await repos.events.ingest(con, "mock_a", event_id, pcid, etype, 0.0, {})
        if row_id is None:
            return None                          # id-level duplicate: never became a ledger row
        ev = await con.fetchrow("SELECT * FROM provider_events WHERE id=$1", row_id)
        await apply_one(con, repos, ev, RealClock(), wrapup_s=8.0)
        return row_id


async def test_duplicate_answered_applied_once(db, repos, campaign_id):
    """ANSWERED x3 (content-level dup, distinct ids) + COMPLETED -> exactly one
    ANSWERED-driven transition (into CONNECTED) and one COMPLETED transition.
    Also exercises id-level dedup: the exact same event id re-delivered never
    becomes a second ledger row at all."""
    call_id, agent_id, _borrower_id = await _make_initiated_call(db, repos, campaign_id)

    await _ingest_and_apply(db, repos, "ev-answered-1", "PCID-1", "ANSWERED")
    await _ingest_and_apply(db, repos, "ev-answered-1", "PCID-1", "ANSWERED")  # id-level dup
    await _ingest_and_apply(db, repos, "ev-answered-2", "PCID-1", "ANSWERED")  # content-level dup
    await _ingest_and_apply(db, repos, "ev-answered-3", "PCID-1", "ANSWERED")  # content-level dup
    await _ingest_and_apply(db, repos, "ev-completed", "PCID-1", "COMPLETED")

    async with db.tx() as con:
        call = await con.fetchrow("SELECT * FROM calls WHERE id=$1", call_id)
        n_rows = await con.fetchval(
            "SELECT count(*) FROM provider_events WHERE call_id=$1", call_id)
        n_state_changing = await con.fetchval(
            "SELECT count(*) FROM provider_events WHERE call_id=$1 AND anomaly IS NULL", call_id)
        agent = await con.fetchrow("SELECT * FROM agents WHERE id=$1", agent_id)

    assert call["state"] == "COMPLETED"
    assert n_rows == 4                 # ev-answered-1 (1 row, id dedup ate the retry), -2, -3, completed
    assert n_state_changing == 2       # one ANSWERED that actually bridged, one COMPLETED
    assert agent["state"] == "WRAP_UP"  # bridged exactly once, then completed exactly once
