"""Property test: whatever order {RINGING, ANSWERED, COMPLETED} are delivered
in, and however many duplicates are mixed in, the call ends up COMPLETED
exactly once and the agent is bridged at most once.

The test bodies run their own `asyncio.run()` inside a plain (sync) function
decorated with `@given` — the well-supported way to combine Hypothesis with
asyncio, and it sidesteps any question of how Hypothesis's example-replay
loop interacts with pytest-asyncio's per-test event loop. Each example gets
its own truncate + fresh call, so examples cannot interfere with each other.
"""
import asyncio
import os
import random

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from smartdialer.clock import RealClock
from smartdialer.db import Db
from smartdialer.domain.enums import AgentState, CallState
from smartdialer.repo import Repos
from smartdialer.worker.event_applier import apply_one

DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL", os.environ.get("DATABASE_URL",
                                         "postgresql://dialer:dialer@localhost:5432/dialer"))
EVENTS = ["RINGING", "ANSWERED", "COMPLETED"]


async def _run_example(order: list[str], extra_dupes: int, seed: int) -> None:
    db = await Db.connect(DATABASE_URL, min_size=1, max_size=5)
    repos = Repos(db)
    try:
        async with db.tx() as con:
            await con.execute(
                "TRUNCATE provider_events, pacing_decisions, calls, borrowers, agents, "
                "campaign_counters, campaigns RESTART IDENTITY CASCADE")
            campaign_id = await con.fetchval(
                "INSERT INTO campaigns (name) VALUES ('prop_events') RETURNING id")
            agent_id = await repos.agents.create(con, campaign_id, "a1")
            await repos.agents.login(con, agent_id)
            await repos.borrowers.seed_many(con, campaign_id, ["+15559990000"])
            b = await repos.borrowers.claim_one(con, campaign_id, "w1", 9e9)
            call_id = await repos.calls.create(con, campaign_id, agent_id, b["id"], "mock_a", 1,
                                                f"{campaign_id}:{b['id']}:1", "w1", 9e9)
            await repos.calls.advance(con, call_id, CallState.INITIATED,
                                       provider_call_id="PCID", lease_until=9e9)
            await repos.agents.force_state(con, agent_id, AgentState.DIALING,
                                            worker="w1", lease_until=9e9, call_id=call_id)

        rng = random.Random(seed)
        stream = list(order)
        for _ in range(extra_dupes):
            stream.insert(rng.randrange(len(stream) + 1), rng.choice(stream))

        for i, etype in enumerate(stream):
            async with db.tx() as con:
                row_id = await repos.events.ingest(con, "mock_a", f"ev-{i}", "PCID", etype, 0.0, {})
                if row_id is None:
                    continue
                ev = await con.fetchrow("SELECT * FROM provider_events WHERE id=$1", row_id)
                await apply_one(con, repos, ev, RealClock(), wrapup_s=8.0)

        async with db.tx() as con:
            call = await con.fetchrow("SELECT * FROM calls WHERE id=$1", call_id)
            bridges = await con.fetchval(
                "SELECT count(*) FROM provider_events WHERE call_id=$1 "
                "AND event_type='ANSWERED' AND anomaly IS NULL", call_id)

        assert call["state"] == "COMPLETED", (order, extra_dupes, seed, call["state"])
        assert call["state_rank"] == 6
        assert bridges <= 1
    finally:
        await db.close()


@settings(max_examples=20, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(order=st.permutations(EVENTS), extra_dupes=st.integers(0, 3), seed=st.integers(0, 100_000))
def test_any_order_reaches_completed_exactly_once(order, extra_dupes, seed):
    asyncio.run(_run_example(list(order), extra_dupes, seed))
