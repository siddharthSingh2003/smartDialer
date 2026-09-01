import asyncio
import csv
import os
import random
import time

from ..allocator.allocator import CallAllocator
from ..clock import VirtualClock
from ..config import Settings
from ..db import Db
from ..ids import worker_id
from ..pacing.predictive import PredictivePacing
from ..pacing.progressive import ProgressivePacing
from ..providers.base import WebhookSink
from ..providers.mock_a import MockProviderA
from ..providers.mock_b import MockProviderB
from ..providers.registry import ProviderRegistry
from ..repo import Repos
from ..safety.controller import SafetyController
from ..worker import event_applier, pacing_loop, reaper
from .world import sample_talk_time, setup_campaign

CSV_COLUMNS = [
    "tick", "sim_time", "agents_available", "agents_reserved", "agents_dialing",
    "agents_connected", "agents_wrapup", "utilization", "calls_initiated_cum",
    "calls_connected_cum", "calls_abandoned_cum", "ringing", "p_answer_point",
    "p_answer_lb", "samples", "requested", "approved", "reason_code",
    "abandon_rate_5m", "provider_a_health", "provider_b_health",
]

CUM_QUERY = """
SELECT
    count(*) FILTER (WHERE initiated_at IS NOT NULL) AS initiated_cum,
    count(*) FILTER (WHERE state = 'COMPLETED')       AS connected_cum,
    count(*) FILTER (WHERE state = 'ABANDONED')        AS abandoned_cum
  FROM calls WHERE campaign_id = $1
"""


async def _row(db, campaign_id: int, tick_i: int, sim_time: float, snapshot, decision) -> dict:
    async with db.tx() as con:
        cum = await con.fetchrow(CUM_QUERY, campaign_id)

    total = (snapshot.agents_available + snapshot.agents_reserved + snapshot.agents_dialing
             + snapshot.agents_connected + snapshot.agents_wrapup)
    busy = total - snapshot.agents_available
    utilization = (busy / total) if total else 0.0
    health = snapshot.provider_health

    return {
        "tick": tick_i, "sim_time": round(sim_time, 3),
        "agents_available": snapshot.agents_available,
        "agents_reserved": snapshot.agents_reserved,
        "agents_dialing": snapshot.agents_dialing,
        "agents_connected": snapshot.agents_connected,
        "agents_wrapup": snapshot.agents_wrapup,
        "utilization": round(utilization, 4),
        "calls_initiated_cum": cum["initiated_cum"],
        "calls_connected_cum": cum["connected_cum"],
        "calls_abandoned_cum": cum["abandoned_cum"],
        "ringing": snapshot.calls_ringing,
        "p_answer_point": round(snapshot.answer_rate_point, 4),
        "p_answer_lb": round(snapshot.answer_rate_lb, 4),
        "samples": snapshot.answer_samples,
        "requested": decision.requested,
        "approved": decision.approved,
        "reason_code": str(decision.reason),
        "abandon_rate_5m": round(snapshot.abandon_rate_5m, 4),
        "provider_a_health": round(health.get("mock_a", 0.0), 3),
        "provider_b_health": round(health.get("mock_b", 0.0), 3),
    }


def write_csv(out_dir: str, scenario_name: str, rows: list[dict]) -> str:
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{scenario_name}.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        w.writeheader()
        w.writerows(rows)
    return path


async def run_scenario(scenario, seed: int = 42, out_dir: str | None = None,
                        database_url: str | None = None,
                        tick_ms: int = 250) -> list[dict]:
    """Drives one scenario end to end on a VirtualClock, against real Postgres
    (not a stub) — the allocator, safety controller, repos, and DB are the
    exact production code path. See ARCHITECTURE.md §13.1."""
    cfg = Settings(database_url=database_url) if database_url else Settings()
    # Seeded at real wall-clock time: lease_expires_at is written to Postgres
    # via to_timestamp(), which the reaper compares against Postgres' now().
    # See tests/conftest.py for the same note.
    clock = VirtualClock(start=time.time())
    # generous max_size: allocate_batch now places its calls concurrently
    # (see allocator/allocator.py), so a burst of N approved calls can want
    # close to N connections at once.
    db = await Db.connect(cfg.database_url, min_size=2, max_size=40)
    repos = Repos(db)

    campaign_id = await setup_campaign(db, repos, scenario, seed)

    rng = random.Random(seed)
    sink = WebhookSink(db, repos.events)
    provider_a = MockProviderA(clock, sink, seed=seed)
    provider_b = MockProviderB(clock, sink, seed=seed + 1)
    registry = ProviderRegistry([provider_a, provider_b])

    def rate_at(t: float) -> float:
        return scenario.answer_rate(t) if callable(scenario.answer_rate) else scenario.answer_rate

    def talk_at(t: float) -> float:
        mean = scenario.talk_time_s(t) if callable(scenario.talk_time_s) else scenario.talk_time_s
        return sample_talk_time(mean, rng)

    for p in (provider_a, provider_b):
        p.answer_probability = lambda: rate_at(clock.now())
        p.talk_time_sampler = lambda: talk_at(clock.now())

    for inj in scenario.injections:
        inj.schedule(clock, db, repos, registry, campaign_id)

    wid = worker_id()
    allocator = CallAllocator(db, repos, registry, clock, cfg, wid)
    controller = SafetyController(cfg.safety, allocator, repos.decisions, clock)
    engine = PredictivePacing() if scenario.mode == "PREDICTIVE" else ProgressivePacing()

    rows: list[dict] = []
    tick_i = 0
    tick_s = tick_ms / 1000.0
    reap_every = max(1, round(1.0 / cfg.reaper_hz / tick_s))
    start_time = clock.now()

    # The tick loop runs as its own task, driven by `clock.run_until()` in
    # this coroutine. Its body is wrapped in `clock.critical()` — see the
    # VirtualClock docstring: without it, a background call's already-running
    # talk timer could cause the driver to jump the clock far ahead while
    # this tick's own (clock-free, but real-I/O-bound) work is still settling,
    # ending the run before the tick ever returns.
    async def loop():
        nonlocal tick_i
        while True:
            await clock.sleep(tick_s)
            async with clock.critical():
                tick_i += 1
                snapshot, request, decision, cancelled = await pacing_loop.tick(
                    db, repos, engine, controller, registry, clock, campaign_id, sim_tick=tick_i)
                await event_applier.drain(db, repos, clock, wrapup_s=cfg.wrapup_s)
                if tick_i % reap_every == 0:
                    await reaper.reap(db, repos, clock, registry)
                rows.append(await _row(db, campaign_id, tick_i, clock.now() - start_time,
                                        snapshot, decision))

    task = asyncio.ensure_future(loop())
    # run_until takes an ABSOLUTE target; the clock is seeded at time.time()
    # (see the note above), so the target is "now + duration", not the bare
    # duration.
    await clock.run_until(clock.now() + scenario.duration_s)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Cancel any provider call-lifecycle tasks still in flight (calls whose
    # talk time hadn't finished when the sim ended) so they don't fire a
    # "pool is closing" warning against the connection pool we're about to
    # close, and don't outlive this function as orphaned tasks.
    for provider in (provider_a, provider_b):
        for t in list(provider.tasks):
            t.cancel()
    for provider in (provider_a, provider_b):
        if provider.tasks:
            await asyncio.gather(*provider.tasks, return_exceptions=True)

    if out_dir:
        write_csv(out_dir, scenario.name, rows)

    await db.close()
    return rows
