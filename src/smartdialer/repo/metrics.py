from ..domain.models import MetricsSnapshot
from ..pacing.estimators import wilson_bounds

AGENT_COUNTS_NAIVE = """
SELECT state, count(*) AS n FROM agents WHERE campaign_id = $1 GROUP BY state
"""

CALL_COUNTS_NAIVE = """
SELECT
    count(*) FILTER (WHERE state IN ('INITIATED','RINGING')) AS ringing,
    count(*) FILTER (WHERE state = 'CONNECTED')               AS connected
  FROM calls WHERE campaign_id = $1
"""

COUNTERS_ROW = "SELECT * FROM campaign_counters WHERE campaign_id = $1"

# Windows below are sample-count based, not wall-clock. That is deliberate:
# calls run under a VirtualClock that can compress 30 real minutes into ~2
# seconds, so a "last 5 minutes" WHERE clause on `now()` would be meaningless
# for the simulator and is avoided everywhere in the metrics path.
RECENT_RESOLVED = """
SELECT state,
       (answered_at IS NOT NULL) AS answered,
       EXTRACT(EPOCH FROM (answered_at - initiated_at)) AS setup_s,
       EXTRACT(EPOCH FROM (terminal_at - connected_at)) AS talk_s
  FROM calls
 WHERE campaign_id = $1 AND terminal_at IS NOT NULL AND initiated_at IS NOT NULL
 ORDER BY id DESC
 LIMIT $2
"""

CAMPAIGN = "SELECT is_active FROM campaigns WHERE id = $1"

WILSON_Z = 1.2816  # 90% one-sided
WINDOW = 300


class MetricsRepo:
    async def _resolved_stats(self, con, campaign_id: int, window: int = WINDOW):
        rows = await con.fetch(RECENT_RESOLVED, campaign_id, window)
        n = len(rows)
        answered = sum(1 for r in rows if r["answered"])
        point = (answered / n) if n else 0.0
        lb, ub = wilson_bounds(point, n, WILSON_Z)
        # EXTRACT(EPOCH FROM interval) returns Postgres `numeric`, which
        # asyncpg decodes as decimal.Decimal — cast to float immediately so
        # MetricsSnapshot's numeric fields stay a consistent float, not a
        # value that silently breaks arithmetic against floats downstream
        # (pacing/predictive.py mixes these with plain floats every tick).
        setups = [float(r["setup_s"]) for r in rows if r["setup_s"] is not None]
        talks = [float(r["talk_s"]) for r in rows if r["talk_s"] is not None]
        abandoned = sum(1 for r in rows if r["state"] == "ABANDONED")
        completed = sum(1 for r in rows if r["state"] == "COMPLETED")
        denom = abandoned + completed
        return {
            "answer_rate_point": point,
            "answer_rate_lb": lb,
            "answer_rate_ub": ub,
            "answer_samples": n,
            "avg_setup_s": (sum(setups) / len(setups)) if setups else 3.0,
            "avg_talk_s": (sum(talks) / len(talks)) if talks else 90.0,
            "abandon_rate_5m": (abandoned / denom) if denom else 0.0,
        }

    async def snapshot_naive(self, con, campaign_id: int, clock) -> MetricsSnapshot:
        """count(*) GROUP BY — the Phase 3 baseline. Degrades at scale; see §15."""
        agent_rows = await con.fetch(AGENT_COUNTS_NAIVE, campaign_id)
        counts = {r["state"]: r["n"] for r in agent_rows}
        call_row = await con.fetchrow(CALL_COUNTS_NAIVE, campaign_id)
        active = await con.fetchval(CAMPAIGN, campaign_id)
        stats = await self._resolved_stats(con, campaign_id)
        return MetricsSnapshot(
            campaign_id=campaign_id, ts=clock.now(),
            agents_available=counts.get("AVAILABLE", 0),
            agents_reserved=counts.get("RESERVED", 0),
            agents_dialing=counts.get("DIALING", 0),
            agents_connected=counts.get("CONNECTED", 0),
            agents_wrapup=counts.get("WRAP_UP", 0),
            calls_ringing=call_row["ringing"],
            calls_connected=call_row["connected"],
            campaign_active=bool(active),
            **stats,
        )

    async def snapshot_fast(self, con, campaign_id: int, clock) -> MetricsSnapshot:
        """Single-row read from the trigger-maintained campaign_counters table.
        O(1) regardless of agent/call count — the Phase 12 fix."""
        row = await con.fetchrow(COUNTERS_ROW, campaign_id)
        active = await con.fetchval(CAMPAIGN, campaign_id)
        stats = await self._resolved_stats(con, campaign_id)
        if row is None:
            counts = dict(agents_available=0, agents_reserved=0, agents_dialing=0,
                          agents_connected=0, agents_wrapup=0, calls_ringing=0, calls_connected=0)
        else:
            counts = dict(row)
        return MetricsSnapshot(
            campaign_id=campaign_id, ts=clock.now(),
            agents_available=counts["agents_available"],
            agents_reserved=counts["agents_reserved"],
            agents_dialing=counts["agents_dialing"],
            agents_connected=counts["agents_connected"],
            agents_wrapup=counts["agents_wrapup"],
            calls_ringing=counts["calls_ringing"],
            calls_connected=counts["calls_connected"],
            campaign_active=bool(active),
            **stats,
        )

    async def snapshot(self, con, campaign_id: int, clock, fast: bool = True) -> MetricsSnapshot:
        if fast:
            return await self.snapshot_fast(con, campaign_id, clock)
        return await self.snapshot_naive(con, campaign_id, clock)
