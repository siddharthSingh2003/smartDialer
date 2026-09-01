"""A terminal live-view of one campaign's state. Not part of the graded
architecture (ARCHITECTURE.md is explicit: no UI) — this is an ops
convenience for watching `smartdialer worker` do its work. See
live_status.py, shared with the web dashboard (api/dashboard.py)."""
import asyncio
import time

from .db import Db
from .live_status import campaign_snapshot

CLEAR = "\x1b[2J\x1b[H"


def _bar(counts: dict[str, int], nonzero_only: bool = True) -> str:
    parts = [f"{k} {v}" for k, v in counts.items() if v or not nonzero_only]
    return "  ".join(parts) if parts else "(none)"


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
    decisions = snap["decisions"]
    if decisions:
        d = decisions[0]
        lines.append(f"Latest pacing decision   requested={d['requested']}  "
                      f"approved={d['approved']}  reason={d['reason_code']}")
    else:
        lines.append("Latest pacing decision   (none yet)")
    lines.append("")
    lines.append("Recent provider events")
    if snap["events"]:
        for e in snap["events"][:6]:
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
                snap = await campaign_snapshot(con, campaign_id, recent_decisions=1, recent_events=6)
            print(CLEAR + _render(campaign_id, snap), flush=True)
            await asyncio.sleep(interval_s)
    finally:
        await db.close()
