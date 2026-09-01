import asyncio
import logging
import signal

from ..allocator.allocator import CallAllocator
from ..clock import RealClock
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
from . import event_applier, reaper
from .pacing_loop import run_forever as pacing_run_forever

logger = logging.getLogger(__name__)


async def event_applier_loop(db, repos, clock, wrapup_s: float, period_s: float = 0.25) -> None:
    while True:
        await clock.sleep(period_s)
        try:
            await event_applier.drain(db, repos, clock, wrapup_s=wrapup_s)
        except Exception:
            logger.exception("event applier tick failed")


def build_worker(db, repos, clock, cfg: Settings, wid: str, mode: str = "PREDICTIVE"):
    """Wires one worker's dependency graph. Every worker binary is identical —
    which one ends up running the pacing tick is decided at runtime by the
    advisory-lock leader election in pacing_loop.py, not by configuration."""
    sink = WebhookSink(db, repos.events)
    registry = ProviderRegistry([MockProviderA(clock, sink), MockProviderB(clock, sink)])
    allocator = CallAllocator(db, repos, registry, clock, cfg, wid)
    controller = SafetyController(cfg.safety, allocator, repos.decisions, clock)
    engine = PredictivePacing() if mode == "PREDICTIVE" else ProgressivePacing()
    return registry, allocator, controller, engine


async def run_worker(campaign_id: int, cfg: Settings | None = None, wid: str | None = None,
                      clock=None, db=None, mode: str = "PREDICTIVE") -> None:
    cfg = cfg or Settings()
    clock = clock or RealClock()
    wid = wid or worker_id()
    owns_db = db is None
    db = db or await Db.connect(cfg.database_url)
    repos = Repos(db)

    # Keep campaigns.mode in sync with what this worker actually runs — it
    # otherwise still shows the schema default (PROGRESSIVE) forever, which
    # is misleading to anything reading it back (e.g. `smartdialer watch`).
    async with db.tx() as con:
        await con.execute("UPDATE campaigns SET mode = $2 WHERE id = $1", campaign_id, mode)

    registry, allocator, controller, engine = build_worker(db, repos, clock, cfg, wid, mode)

    tasks = [
        asyncio.create_task(pacing_run_forever(
            db, repos, engine, controller, registry, clock, campaign_id, tick_ms=cfg.tick_ms)),
        asyncio.create_task(event_applier_loop(db, repos, clock, cfg.wrapup_s)),
        asyncio.create_task(reaper.run_forever(db, repos, registry, clock, hz=cfg.reaper_hz)),
    ]

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass   # Windows: no signal handlers on the event loop; Ctrl+C raises KeyboardInterrupt instead

    logger.info("worker %s started for campaign %s (mode=%s)", wid, campaign_id, mode)
    try:
        await stop.wait()
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if owns_db:
            await db.close()
