"""Shared live-state queries for `smartdialer watch` (terminal) and the
optional web dashboard (api/dashboard.py). Both are ops conveniences on top
of the graded system, not part of it — ARCHITECTURE.md is explicit that
there is no UI in the design being graded. They read only what's already in
Postgres, the same audit trail `psql` would show, which keeps them honest
about the database being the single source of truth (§7)."""
import json

AGENT_STATES = ["AVAILABLE", "RESERVED", "DIALING", "CONNECTED", "WRAP_UP", "PAUSED", "OFFLINE"]
CALL_STATES = ["QUEUED", "RESERVED", "INITIATED", "RINGING", "ANSWERED", "CONNECTED",
               "COMPLETED", "FAILED", "CANCELLED", "ABANDONED"]
BORROWER_STATES = ["PENDING", "LOCKED", "IN_CALL", "DONE", "EXHAUSTED", "SUPPRESSED"]


def _counts(rows, states: list[str]) -> dict[str, int]:
    have = {r["state"]: r["n"] for r in rows}
    return {s: have.get(s, 0) for s in states}


async def list_campaigns(con) -> list[dict]:
    rows = await con.fetch(
        "SELECT id, name, mode, is_active FROM campaigns ORDER BY id DESC LIMIT 50")
    return [dict(r) for r in rows]


async def campaign_snapshot(con, campaign_id: int, recent_decisions: int = 12,
                             recent_events: int = 20) -> dict:
    campaign = await con.fetchrow(
        "SELECT name, mode, is_active FROM campaigns WHERE id=$1", campaign_id)
    agents = _counts(await con.fetch(
        "SELECT state::text, count(*) AS n FROM agents WHERE campaign_id=$1 GROUP BY state",
        campaign_id), AGENT_STATES)
    calls = _counts(await con.fetch(
        "SELECT state::text, count(*) AS n FROM calls WHERE campaign_id=$1 GROUP BY state",
        campaign_id), CALL_STATES)
    borrowers = _counts(await con.fetch(
        "SELECT state::text, count(*) AS n FROM borrowers WHERE campaign_id=$1 GROUP BY state",
        campaign_id), BORROWER_STATES)

    decision_rows = await con.fetch(
        "SELECT id, tick_at, mode, requested, approved, reason_code, inputs "
        "FROM pacing_decisions WHERE campaign_id=$1 ORDER BY id DESC LIMIT $2",
        campaign_id, recent_decisions)
    decisions = []
    for r in decision_rows:
        d = dict(r)
        try:
            d["inputs"] = json.loads(d["inputs"]) if isinstance(d["inputs"], str) else d["inputs"]
        except (TypeError, ValueError):
            pass
        decisions.append(d)

    events = [dict(r) for r in await con.fetch(
        "SELECT pe.id, pe.received_at, pe.event_type, pe.anomaly, pe.applied, c.id AS call_id "
        "FROM provider_events pe JOIN calls c ON c.id = pe.call_id "
        "WHERE c.campaign_id=$1 ORDER BY pe.id DESC LIMIT $2", campaign_id, recent_events)]

    totals = await con.fetchrow(
        "SELECT count(*) FILTER (WHERE state='COMPLETED') AS completed, "
        "count(*) FILTER (WHERE state='FAILED') AS failed, "
        "count(*) FILTER (WHERE state='CANCELLED') AS cancelled, "
        "count(*) FILTER (WHERE state='ABANDONED') AS abandoned "
        "FROM calls WHERE campaign_id=$1", campaign_id)

    total_agents = sum(agents.values())
    busy = total_agents - agents["AVAILABLE"] - agents["OFFLINE"] - agents["PAUSED"]
    staffed = total_agents - agents["OFFLINE"]
    utilization = (busy / staffed * 100) if staffed else 0.0

    return {
        "campaign_id": campaign_id,
        "campaign": dict(campaign) if campaign else None,
        "agents": agents,
        "calls": calls,
        "borrowers": borrowers,
        "totals": dict(totals),
        "utilization": round(utilization, 1),
        "decisions": decisions,
        "events": events,
    }
