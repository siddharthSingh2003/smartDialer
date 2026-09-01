import pytest

from smartdialer.domain.enums import AgentState, CallState

pytestmark = pytest.mark.asyncio


async def test_snapshot_numeric_fields_are_float_not_decimal(db, repos, campaign_id, clock):
    """Regression test: EXTRACT(EPOCH FROM interval) returns Postgres
    `numeric`, which asyncpg decodes as decimal.Decimal unless explicitly
    cast (repo/metrics.py::_resolved_stats). A Decimal avg_setup_s/avg_talk_s
    silently breaks pacing/predictive.py's arithmetic the moment it mixes
    with a plain float — only visible once a call has actually resolved
    through the real DB, not from a hand-built MetricsSnapshot in a unit
    test, which is why this needs the real fixtures."""
    async with db.tx() as con:
        agent_id = await repos.agents.create(con, campaign_id, "a1")
        await repos.agents.login(con, agent_id)
        await repos.borrowers.seed_many(con, campaign_id, ["+15550009999"])
        b = await repos.borrowers.claim_one(con, campaign_id, "w1", 9e9)
        call_id = await repos.calls.create(con, campaign_id, agent_id, b["id"], "mock_a", 1,
                                            f"{campaign_id}:{b['id']}:1", "w1", 9e9)
        await repos.calls.advance(con, call_id, CallState.INITIATED,
                                   provider_call_id="PCID-M", lease_until=9e9)
        await repos.agents.force_state(con, agent_id, AgentState.DIALING,
                                        worker="w1", lease_until=9e9, call_id=call_id)
        await repos.calls.advance(con, call_id, CallState.RINGING)
        await repos.calls.advance(con, call_id, CallState.ANSWERED)
        await repos.calls.advance(con, call_id, CallState.CONNECTED)
        await repos.calls.advance(con, call_id, CallState.COMPLETED)

    async with db.tx() as con:
        snap = await repos.metrics.snapshot(con, campaign_id, clock, fast=False)

    assert isinstance(snap.avg_setup_s, float)
    assert isinstance(snap.avg_talk_s, float)

    # and the arithmetic that broke on Decimal must actually run cleanly
    from smartdialer.pacing.predictive import PredictivePacing
    req = PredictivePacing().decide(snap)
    assert isinstance(req.n, int)
