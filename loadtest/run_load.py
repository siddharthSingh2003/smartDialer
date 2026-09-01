"""Phase 12 load test. The deliverable is not a big number — it is *where it
degrades and why* (ARCHITECTURE.md §15). This measures the two things named
there directly: the pacing snapshot query (naive count(*)/GROUP BY vs the
O(1) campaign_counters table) and CAS contention on agent reservation, at
increasing campaign size.
"""
import argparse
import asyncio
import csv
import os
import random
import sys
import time

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from smartdialer.clock import RealClock
from smartdialer.db import Db
from smartdialer.repo import Repos

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://dialer:dialer@localhost:5432/dialer")

AGENT_STATE_MIX = (["AVAILABLE"] * 55 + ["RESERVED"] * 10 + ["DIALING"] * 15
                    + ["CONNECTED"] * 15 + ["WRAP_UP"] * 5)
CALL_STATE_MIX = (["INITIATED"] * 30 + ["RINGING"] * 20 + ["CONNECTED"] * 20
                   + ["COMPLETED"] * 30)


def _pctile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    k = max(0, min(len(values) - 1, int(round(p / 100 * (len(values) - 1)))))
    return round(values[k], 3)


async def _seed(con, campaign_id: int, n_agents: int) -> None:
    agent_rows = [(campaign_id, f"a{i}", AGENT_STATE_MIX[i % len(AGENT_STATE_MIX)])
                  for i in range(n_agents)]
    await con.executemany(
        "INSERT INTO agents (campaign_id, ext_ref, state) VALUES ($1,$2,$3)", agent_rows)

    n_borrowers = max(n_agents * 3, 300)
    borrower_rows = [(campaign_id, f"+1555{i:07d}") for i in range(n_borrowers)]
    await con.executemany(
        "INSERT INTO borrowers (campaign_id, phone) VALUES ($1,$2)", borrower_rows)
    borrower_ids = [r["id"] for r in await con.fetch(
        "SELECT id FROM borrowers WHERE campaign_id=$1", campaign_id)]

    n_calls = int(n_agents * 1.5)
    rng = random.Random(1)
    call_rows = [
        (campaign_id, rng.choice(borrower_ids), "mock_a",
         CALL_STATE_MIX[i % len(CALL_STATE_MIX)], f"{campaign_id}:{i}:1")
        for i in range(n_calls)
    ]
    await con.executemany(
        "INSERT INTO calls (campaign_id, borrower_id, provider, state, idempotency_key) "
        "VALUES ($1,$2,$3,$4,$5)", call_rows)


async def _time_snapshot(db, repos, campaign_id: int, fast: bool, n: int = 60) -> list[float]:
    clock = RealClock()
    latencies = []
    for _ in range(n):
        t0 = time.perf_counter()
        async with db.tx() as con:
            await repos.metrics.snapshot(con, campaign_id, clock, fast=fast)
        latencies.append((time.perf_counter() - t0) * 1000)
    return latencies


async def _time_cas_contention(db, repos, campaign_id: int, n_workers: int = 30):
    async with db.tx() as con:
        cands = await con.fetch(
            "SELECT id FROM agents WHERE campaign_id=$1 AND state='AVAILABLE' LIMIT $2",
            campaign_id, n_workers)
    latencies: list[float] = []
    losses = 0

    async def attempt(agent_id: int) -> None:
        nonlocal losses
        async with db.tx() as con:
            row = await con.fetchrow("SELECT id, version FROM agents WHERE id=$1", agent_id)
            t0 = time.perf_counter()
            ok = await repos.agents.reserve(con, row["id"], row["version"], "loadtest", 9e9)
            latencies.append((time.perf_counter() - t0) * 1000)
            if not ok:
                losses += 1

    await asyncio.gather(*[attempt(c["id"]) for c in cands])
    rate = losses / max(1, len(cands))
    return latencies, rate


async def run_for_scale(n_agents: int) -> dict:
    db = await Db.connect(DATABASE_URL, min_size=2, max_size=35)
    repos = Repos(db)
    async with db.tx() as con:
        await con.execute(
            "TRUNCATE provider_events, pacing_decisions, calls, borrowers, agents, "
            "campaign_counters, campaigns RESTART IDENTITY CASCADE")
        campaign_id = await con.fetchval(
            "INSERT INTO campaigns (name) VALUES ($1) RETURNING id", f"load-{n_agents}")
        await _seed(con, campaign_id, n_agents)

    naive = await _time_snapshot(db, repos, campaign_id, fast=False)
    fast = await _time_snapshot(db, repos, campaign_id, fast=True)
    cas_latencies, cas_loss_rate = await _time_cas_contention(db, repos, campaign_id)

    await db.close()
    return {
        "agents": n_agents,
        "snapshot_naive_p50_ms": _pctile(naive, 50),
        "snapshot_naive_p95_ms": _pctile(naive, 95),
        "snapshot_counters_p50_ms": _pctile(fast, 50),
        "snapshot_counters_p95_ms": _pctile(fast, 95),
        "cas_reserve_p95_ms": _pctile(cas_latencies, 95),
        "cas_loss_rate": round(cas_loss_rate, 3),
        "tick_deadline_ms": 250,
        "naive_hits_deadline": _pctile(naive, 95) < 250,
    }


async def main(agent_counts: list[int], out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    rows = []
    for n in agent_counts:
        print(f"scale n_agents={n} ...")
        row = await run_for_scale(n)
        rows.append(row)
        print(f"  {row}")

    path = os.path.join(out_dir, "scale.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    xs = [r["agents"] for r in rows]
    plt.figure(figsize=(8, 4.5))
    plt.plot(xs, [r["snapshot_naive_p95_ms"] for r in rows], marker="o",
              color="#dc2626", label="naive count(*)/GROUP BY p95")
    plt.plot(xs, [r["snapshot_counters_p95_ms"] for r in rows], marker="o",
              color="#16a34a", label="campaign_counters (O(1)) p95")
    plt.axhline(250, color="#111827", linestyle="--", linewidth=1, label="250ms tick deadline")
    plt.xlabel("agents in campaign")
    plt.ylabel("pacing snapshot latency (ms)")
    plt.title("Snapshot latency vs scale: the Phase 12 fix")
    plt.legend()
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "scale_latency.png"), dpi=130)
    print(f"wrote {path} and {out_dir}/scale_latency.png")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--agents", type=int, nargs="*", default=[100, 500, 1000, 2000])
    parser.add_argument("--out", default="loadtest/results")
    args = parser.parse_args()
    asyncio.run(main(args.agents, args.out))
