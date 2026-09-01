import time

import pytest

from smartdialer.clock import VirtualClock
from smartdialer.domain.enums import AgentState
from smartdialer.worker.reaper import reap

pytestmark = pytest.mark.asyncio


async def test_reap_compares_against_the_clocks_own_now_not_postgres_wall_clock(
        db, repos, campaign_id):
    """Regression test for a systemic bug: leases are written as
    to_timestamp(clock.now() + duration); the reaper used to compare them
    against Postgres' bare now(). That happens to work under RealClock
    (clock time == wall time) but breaks under the simulator's VirtualClock,
    which is seeded far ahead of real elapsed time and races further ahead
    as ticks process — a lease computed from an already-advanced virtual
    "now" can require far more REAL time to satisfy than the simulation's
    entire run takes, so it would simply never expire.

    This reproduces that mismatch directly: a clock seeded 1000 (virtual)
    seconds ahead of real wall-clock time writes a lease that is already
    expired relative to ITS OWN now(), but is nowhere near expired relative
    to Postgres' real now() (which is ~1000s earlier). Only a fix that
    compares against the clock's own now() reaps it."""
    future_clock = VirtualClock(start=time.time() + 1000)

    async with db.tx() as con:
        agent_id = await repos.agents.create(con, campaign_id, "a1")
        await repos.agents.login(con, agent_id)
        await repos.agents.force_state(con, agent_id, AgentState.WRAP_UP,
                                        lease_until=future_clock.now() - 1)

    result = await reap(db, repos, future_clock, registry=None)
    assert result["agents_wrapup_released"] == 1

    async with db.tx() as con:
        agent = await con.fetchrow("SELECT * FROM agents WHERE id=$1", agent_id)
    assert agent["state"] == "AVAILABLE"
