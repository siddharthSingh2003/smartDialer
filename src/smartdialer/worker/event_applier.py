import logging

from ..domain.enums import AgentState, CallState
from ..domain.transitions import AGENT_ON_CALL_STATE, can_apply
from ..providers.base import map_provider_event

logger = logging.getLogger(__name__)


async def _try_bridge(con, repos, row) -> CallState:
    """ANSWERED does not, by itself, mean CONNECTED: the agent has to still be
    the one holding the call. If a crash cost us the agent in between (the
    reaper already reclaimed it), the borrower answered a call with nobody on
    the other end — that is ABANDONED, the compliance event, not a bug to
    hide. See ARCHITECTURE.md §5.1 and the agent state diagram in §4.2."""
    agent = await repos.agents.get(con, row["agent_id"]) if row["agent_id"] else None
    bridged = (agent is not None and agent["current_call_id"] == row["id"]
               and agent["state"] == AgentState.DIALING)
    next_state = CallState.CONNECTED if bridged else CallState.ABANDONED
    row2 = await repos.calls.advance(con, row["id"], next_state)
    return next_state if row2 is not None else CallState.ANSWERED


async def _apply_side_effects(con, repos, row, final_state: CallState, clock, wrapup_s: float) -> None:
    # Side effects belong to the TRANSITION, not the raw event. Because the
    # transition commits at most once per rank level (the state_rank guard in
    # repo/calls.py::ADVANCE), the side effect fires at most once even under
    # duplicate delivery.
    agent_state = AGENT_ON_CALL_STATE.get(final_state)
    if agent_state and row["agent_id"]:
        # WRAP_UP carries a lease as its timer: RELEASE_WRAPUP in
        # worker/reaper.py compares it the same way RESERVED/DIALING leases
        # are compared, which is what keeps this correct under both
        # RealClock (production) and VirtualClock (simulator) — a plain
        # wall-clock "8 real seconds" would be meaningless once the
        # simulator starts compressing time.
        lease_until = clock.now() + wrapup_s if agent_state == AgentState.WRAP_UP else None
        await repos.agents.force_state(con, row["agent_id"], agent_state, lease_until=lease_until)
    if final_state in (CallState.COMPLETED, CallState.FAILED,
                        CallState.CANCELLED, CallState.ABANDONED):
        await repos.borrowers.on_call_terminal(con, row["borrower_id"], str(final_state),
                                                backoff_s=60)


async def apply_one(con, repos, ev, clock, wrapup_s: float) -> None:
    call = await repos.calls.get_by_provider_ref(con, ev["provider"], ev["provider_call_id"])
    if call is None:
        # webhook raced call creation, or the call's row never made it to
        # provider_call_id being set before we crashed. Left unapplied; the
        # next drain cycle retries it, and give_up_unknown bounds the wait.
        await repos.events.mark(con, ev["id"], applied=False, anomaly="UNKNOWN_CALL")
        return

    incoming = CallState(map_provider_event(ev["event_type"]))
    ok, anomaly = can_apply(CallState(call["state"]), incoming)
    if not ok:
        # recorded, deliberately not applied — the audit trail IS the answer
        # when someone asks what happened to a late or duplicate event.
        await repos.events.mark(con, ev["id"], applied=True, anomaly=anomaly, call_id=call["id"])
        return

    row = await repos.calls.advance(con, call["id"], incoming,
                                     provider_call_id=ev["provider_call_id"])
    if row is None:                              # lost to a concurrent applier
        await repos.events.mark(con, ev["id"], applied=True, anomaly="OUT_OF_ORDER",
                                 call_id=call["id"])
        return

    final_state = incoming
    if incoming == CallState.ANSWERED:
        final_state = await _try_bridge(con, repos, row)

    await _apply_side_effects(con, repos, row, final_state, clock, wrapup_s)
    await repos.events.mark(con, ev["id"], applied=True, call_id=call["id"])


async def drain(db, repos, clock, wrapup_s: float = 8.0, limit: int = 200) -> int:
    async with db.tx() as con:
        rows = await repos.events.fetch_unapplied(con, limit)
        for ev in rows:
            try:
                await apply_one(con, repos, ev, clock, wrapup_s)
            except Exception:
                logger.exception("event_applier failed on event id=%s", ev["id"])
        await repos.events.give_up_unknown(con)
    return len(rows)
