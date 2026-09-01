import asyncio
import logging

logger = logging.getLogger(__name__)

# Every lease comparison below uses to_timestamp($1) — the CALLER's clock's
# own now(), not Postgres' bare now() — and that is deliberate, not
# cosmetic. Leases are written as to_timestamp(clock.now() + duration)
# (repo/agents.py, event_applier.py); under RealClock that coincides with
# wall-clock time, so comparing against Postgres' now() would happen to
# work. Under the simulator's VirtualClock it does not: virtual time races
# far ahead of real elapsed time (300 virtual seconds can compress into ~10
# real ones), so a lease written as "virtual-now + 8s" can end up requiring
# far more REAL wall-clock time to satisfy than the simulation's entire real
# run takes — the lease would then simply never expire within the run.
# Comparing against the same clock's own now() instead keeps the write side
# and the read side in the same time reference frame, whichever clock that
# is. (Columns that are just bookkeeping — updated_at, state_changed_at,
# terminal_at — stay on Postgres' now(): nothing else compares them against
# a clock-based deadline.)

# A RESERVED agent whose lease expired is trivially safe to reclaim: no
# provider call was ever placed for it. A DIALING agent is deliberately left
# alone here — it may have a live provider call in flight, so it is only
# released via the call's own terminal transition (from the orphan sweep
# below, or from a late webhook). Reversed ordering would let an agent go
# AVAILABLE while its call is still live, and the next tick could double-book
# it. See ARCHITECTURE.md §9 / §17 "least confident" answer.
RECLAIM_AGENTS = """
UPDATE agents
   SET state = CASE WHEN state = 'RESERVED' THEN 'AVAILABLE' ELSE state END,
       version = version + 1, lease_owner = NULL, lease_expires_at = NULL,
       updated_at = now()
 WHERE state IN ('RESERVED','DIALING') AND lease_expires_at < to_timestamp($1)
RETURNING id, state, current_call_id
"""

FAIL_ORPHAN_CALLS = """
UPDATE calls
   SET state = 'FAILED', state_rank = 6, terminal_at = now(),
       failure_reason = 'LEASE_EXPIRED_NO_EVENTS'
 WHERE terminal_at IS NULL
   AND lease_expires_at < to_timestamp($1)
   AND state IN ('QUEUED','RESERVED','INITIATED')
RETURNING id, agent_id, borrower_id, provider, provider_call_id
"""

# This is the "WRAP_UP --> AVAILABLE: wrap timer elapsed" edge in the agent
# state diagram (§4.2) — nothing else in the system ever fires it.
RELEASE_WRAPUP = """
UPDATE agents
   SET state = 'AVAILABLE', version = version + 1, updated_at = now(),
       state_changed_at = now(), lease_expires_at = NULL, current_call_id = NULL
 WHERE state = 'WRAP_UP' AND lease_expires_at < to_timestamp($1)
RETURNING id
"""


def _schedule_cancel(registry, provider_name: str, provider_call_id: str | None) -> None:
    if not provider_call_id or registry is None:
        return
    try:
        provider = registry.get(provider_name)
    except KeyError:
        return
    asyncio.ensure_future(provider.cancel(provider_call_id))  # fire-and-forget, never block the reaper


async def reap(db, repos, clock, registry=None) -> dict:
    now_ts = clock.now()
    async with db.tx() as con:
        orphans = await con.fetch(FAIL_ORPHAN_CALLS, now_ts)
        for o in orphans:
            _schedule_cancel(registry, o["provider"], o["provider_call_id"])
            if o["agent_id"]:
                await repos.agents.release(con, o["agent_id"])
            await repos.borrowers.reschedule(con, o["borrower_id"], backoff_s=120)

        reclaimed = await con.fetch(RECLAIM_AGENTS, now_ts)
        wrapped_up = await con.fetch(RELEASE_WRAPUP, now_ts)
        stale_locks = await repos.borrowers.reclaim_expired_locks(con)

    return {"orphans_failed": len(orphans), "agents_reclaimed": len(reclaimed),
            "agents_wrapup_released": len(wrapped_up),
            "stale_borrower_locks": len(stale_locks)}


async def run_forever(db, repos, registry, clock, hz: float = 1.0) -> None:
    period = 1.0 / hz
    while True:
        await clock.sleep(period)
        try:
            await reap(db, repos, clock, registry)
        except Exception:
            logger.exception("reaper tick failed")
