"""A terminal live-view of one campaign's state. Not part of the graded
architecture (ARCHITECTURE.md is explicit: no UI) — this is an ops
convenience for watching `smartdialer worker` do its work, reading the same
tables the pacing_decisions audit trail and `psql` would."""
import asyncio
import time

from .db import Db

CLEAR = "\x1b[2J\x1b[H"

AGENT_STATES = ["AVAILABLE", "RESERVED", "DIALING", "CONNECTED", "WRAP_UP", "PAUSED", "OFFLINE"]
CALL_STATES = ["QUEUED", "RESERVED", "INITIATED", "RINGING", "ANSWERED", "CONNECTED",
               "COMPLETED", "FAILED", "CANCELLED", "ABANDONED"]
BORROWER_STATES = ["PENDING", "LOCKED", "IN_CALL", "DONE", "EXHAUSTED", "SUPPRESSED"]


def _counts(rows, states) -> dict[str, int]:
    have = {r["state"]: r["n"] for r in rows}
    return {s: have.get(s, 0) for s in states}


def _bar(counts: dict[str, int], nonzero_only: bool = True) -> str:
    parts = [f"{k} {v}" for k, v in counts.items() if v or not nonzero_only]
    return "  ".join(parts) if parts else "(none)"


async def _snapshot(con, campaign_id: int) -> dict:
    campaign = await con.fetchrow("SELECT name, mode, is_active FROM campaigns WHERE id=$1",
                                   campaign_id)
    agents = _counts(await con.fetch(
        "SELECT state::text, count(*) AS n FROM agents WHERE campaign_id=$1 GROUP BY state",
        campaign_id), AGENT_STATES)
    calls = _counts(await con.fetch(
        "SELECT state::text, count(*) AS n FROM calls WHERE campaign_id=$1 GROUP BY state",
        campaign_id), CALL_STATES)
    borrowers = _counts(await con.fetch(
        "SELECT state::text, count(*) AS n FROM borrowers WHERE campaign_id=$1 GROUP BY state",
        campaign_id), BORROWER_STATES)
    decision = await con.fetchrow(
        "SELECT tick_at, requested, approved, reason_code FROM pacing_decisions "
        "WHERE campaign_id=$1 ORDER BY id DESC LIMIT 1", campaign_id)
    recent_events = await con.fetch(
        "SELECT received_at, event_type, anomaly FROM provider_events pe "
        "JOIN calls c ON c.id = pe.call_id WHERE c.campaign_id=$1 "
        "ORDER BY pe.id DESC LIMIT 6", campaign_id)

    total_agents = sum(agents.values())
    busy = total_agents - agents["AVAILABLE"] - agents["OFFLINE"] - agents["PAUSED"]
    staffed = total_agents - agents["OFFLINE"]
    utilization = (busy / staffed * 100) if staffed else 0.0

    return {"campaign": campaign, "agents": agents, "calls": calls, "borrowers": borrowers,
            "decision": decision, "recent_events": recent_events, "utilization": utilization}


def _render(campaign_id: int, snap: dict) -> str:
    c = snap["campaign"]
    lines = []
    title = f"SmartDialer -- campaign {campaign_id}"
    if c:
        title += f"  ({c['name']}, {c['mode']}, {'active' if c['is_active'] else 'paused'})"
    lines.append(title)
    lines.append(time.strftime("%Y-%m-%d %H:%M:%S"))
    lines.append("-" * 72)
    lines.append(f"Agents        {_bar(snap['agents'])}")
    lines.append(f"Calls         {_bar(snap['calls'])}")
    lines.append(f"Borrowers     {_bar(snap['borrowers'])}")
    lines.append(f"Utilization   {snap['utilization']:.1f}%")
    lines.append("")
    d = snap["decision"]
    if d:
        lines.append(f"Latest pacing decision   requested={d['requested']}  "
                      f"approved={d['approved']}  reason={d['reason_code']}")
    else:
        lines.append("Latest pacing decision   (none yet)")
    lines.append("")
    lines.append("Recent provider events")
    if snap["recent_events"]:
        for e in snap["recent_events"]:
            flag = f"  [{e['anomaly']}]" if e["anomaly"] else ""
            local_ts = e["received_at"].astimezone()   # DB timestamps are UTC; match the header's local clock
            lines.append(f"  {local_ts:%H:%M:%S}  {e['event_type']}{flag}")
    else:
        lines.append("  (none yet)")
    lines.append("")
    lines.append("Ctrl+C to stop")
    return "\n".join(lines)


async def run_watch(campaign_id: int, database_url: str, interval_s: float = 1.5,
                     duration_s: float | None = None) -> None:
    db = await Db.connect(database_url, min_size=1, max_size=3)
    start = time.monotonic()
    try:
        while duration_s is None or time.monotonic() - start < duration_s:
            async with db.tx() as con:
                snap = await _snapshot(con, campaign_id)
            print(CLEAR + _render(campaign_id, snap), flush=True)
            await asyncio.sleep(interval_s)
    finally:
        await db.close()
