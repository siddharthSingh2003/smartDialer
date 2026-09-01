import asyncio
import logging

import typer
import uvicorn

from .config import Settings
from .db import Db
from .logging import setup_logging
from .repo import Repos
from .sim.report import generate_report
from .sim.runner import run_scenario
from .sim.scenarios import SCENARIOS

app = typer.Typer(add_completion=False)


@app.command()
def migrate(database_url: str = typer.Option(None)) -> None:
    """Apply migrations/*.sql in order. Requires the postgres service to be
    reachable; prefer `docker compose exec db psql ... -f migrations/001_init.sql`
    (see README) if you want the plain-SQL path instead."""
    import glob
    import os

    cfg = Settings(database_url=database_url) if database_url else Settings()

    async def _run():
        db = await Db.connect(cfg.database_url)
        here = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        for path in sorted(glob.glob(os.path.join(here, "migrations", "*.sql"))):
            sql = open(path).read()
            async with db.tx() as con:
                await con.execute(sql)
            typer.echo(f"applied {path}")
        await db.close()

    asyncio.run(_run())


@app.command()
def seed(agents: int = 100, borrowers: int = 5000, name: str = "demo",
         database_url: str = typer.Option(None)) -> None:
    """Seed one campaign with logged-in agents and a borrower pool."""
    cfg = Settings(database_url=database_url) if database_url else Settings()

    async def _run():
        db = await Db.connect(cfg.database_url)
        repos = Repos(db)
        async with db.tx() as con:
            campaign_id = await con.fetchval(
                "INSERT INTO campaigns (name) VALUES ($1) RETURNING id", name)
            for i in range(agents):
                aid = await repos.agents.create(con, campaign_id, f"agent-{i}")
                await repos.agents.login(con, aid)
            phones = [f"+1555000{i:05d}" for i in range(borrowers)]
            await repos.borrowers.seed_many(con, campaign_id, phones)
        typer.echo(f"seeded campaign_id={campaign_id} agents={agents} borrowers={borrowers}")
        await db.close()

    asyncio.run(_run())


@app.command()
def worker(campaign_id: int = typer.Option(...), id: str = typer.Option(None),
           mode: str = "PREDICTIVE", database_url: str = typer.Option(None)) -> None:
    """Run one worker binary (pacing leader election + allocation + event
    application + reaper). Run 2-3 of these against the same campaign_id for
    the distributed demo — see ARCHITECTURE.md §3.4."""
    setup_logging(logging.INFO)
    from .worker.runner import run_worker
    cfg = Settings(database_url=database_url) if database_url else Settings()
    asyncio.run(run_worker(campaign_id, cfg=cfg, wid=id, mode=mode))


@app.command()
def watch(campaign_id: int = typer.Option(...), interval: float = 1.5,
          duration: float = None, database_url: str = typer.Option(None)) -> None:
    """Live terminal view of one campaign: agent/call/borrower state counts,
    the latest pacing decision, and recent provider events. Not part of the
    graded architecture (ARCHITECTURE.md is explicit: no UI) — an ops
    convenience for watching a running worker do its work."""
    cfg = Settings(database_url=database_url) if database_url else Settings()
    from .watch import run_watch
    try:
        asyncio.run(run_watch(campaign_id, cfg.database_url, interval_s=interval,
                               duration_s=duration))
    except KeyboardInterrupt:
        pass


@app.command()
def api(host: str = "0.0.0.0", port: int = 8000) -> None:
    """Run the webhook ingest API."""
    setup_logging(logging.INFO)
    uvicorn.run("smartdialer.api.app:app", host=host, port=port)


@app.command()
def sim(scenario: str = "all", seed: int = 42, out: str = "loadtest/results",
        duration: float = None, agents: int = None,
        database_url: str = typer.Option(None)) -> None:
    """Run one scenario (or `all`) on a VirtualClock against real Postgres,
    write per-tick CSVs, and render the report charts."""
    setup_logging(logging.WARNING)
    names = list(SCENARIOS.keys()) if scenario == "all" else [scenario]

    async def _run():
        for name in names:
            sc = SCENARIOS[name]
            if duration is not None:
                sc.duration_s = duration
            if agents is not None:
                sc.agents = agents
            typer.echo(f"running scenario {name} "
                       f"(agents={sc.agents}, duration_s={sc.duration_s}, mode={sc.mode}) ...")
            rows = await run_scenario(sc, seed=seed, out_dir=out, database_url=database_url)
            typer.echo(f"  -> {len(rows)} ticks written to {out}/{name}.csv")
        generate_report(out)
        typer.echo(f"report + charts written to {out}/")

    asyncio.run(_run())


if __name__ == "__main__":
    app()
