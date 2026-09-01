import logging
from dataclasses import replace

logger = logging.getLogger(__name__)


async def tick(db, repos, engine, controller, registry, clock, campaign_id: int,
               sim_tick: int | None = None):
    async with db.tx() as con:
        snapshot = await repos.metrics.snapshot(con, campaign_id, clock)
    if registry is not None:
        snapshot = replace(snapshot, provider_health=registry.health_map())

    request = engine.decide(snapshot)
    decision = await controller.evaluate_and_execute(
        request, snapshot, mode=engine.mode, sim_tick=sim_tick)

    # Free-cancellation pullback (§12.3): if more calls are ringing than the
    # current agent pool can safely support, cancel the excess before anyone
    # answers instead of waiting for it to become an abandon. Ceiling uses
    # agents_staffed (total logged-in), matching S6 — see its comment in
    # safety/rules.py for why agents_available alone would self-trigger on
    # ordinary utilization.
    ceiling = int(snapshot.agents_staffed * controller.limits.max_overdial_ratio)
    allowed_ringing = max(0, ceiling - snapshot.agent_bound_inflight)
    cancelled = 0
    if snapshot.calls_ringing > allowed_ringing:
        cancelled = await controller.cancel_excess_ringing(campaign_id, allowed_ringing)
        if cancelled:
            logger.info("campaign %s: pulled back %d ringing calls", campaign_id, cancelled)

    return snapshot, request, decision, cancelled


async def run_forever(db, repos, engine, controller, registry, clock, campaign_id: int,
                       tick_ms: int = 250, lock_poll_s: float = 1.0, sim_tick_fn=None) -> None:
    """Leader-elected pacing loop. Every worker calls this; `pg_try_advisory_lock`
    ensures exactly one of them is actually ticking a given campaign at a time
    (ARCHITECTURE.md §3.4). Losing the connection (crash) releases the lock
    automatically and another worker takes over within one poll interval."""
    async with db.dedicated() as lock_con:
        leader = False
        try:
            while True:
                if not leader:
                    leader = await db.try_leader(lock_con, campaign_id)
                    if not leader:
                        await clock.sleep(lock_poll_s)
                        continue
                    logger.info("campaign %s: acquired pacing leadership", campaign_id)

                await tick(db, repos, engine, controller, registry, clock, campaign_id,
                           sim_tick=sim_tick_fn() if sim_tick_fn else None)
                await clock.sleep(tick_ms / 1000.0)
        finally:
            if leader:
                await db.release_leader(lock_con, campaign_id)
