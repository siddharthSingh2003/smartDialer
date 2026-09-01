from .enums import AgentState as A
from .enums import CallState as C

RANK: dict[C, int] = {
    C.QUEUED: 0, C.RESERVED: 1, C.INITIATED: 2, C.RINGING: 3,
    C.ANSWERED: 4, C.CONNECTED: 5,
    C.COMPLETED: 6, C.FAILED: 6, C.CANCELLED: 6, C.ABANDONED: 6,
}
TERMINAL_RANK = 6
TERMINAL = {s for s, r in RANK.items() if r == TERMINAL_RANK}

# Any terminal state is legal from any non-terminal state, not just the
# "natural" predecessor. This is deliberate: a provider that drops or never
# sends an intermediate webhook (ANSWERED lost in transit, only COMPLETED
# arrives) still needs its terminal event applied, not rejected as illegal —
# the rank guard already prevents anything terminal from being reopened, and
# once one terminal event is applied every later event is caught by the
# terminal-absorbs-everything rule above regardless of what state it claims.
# The non-terminal progression itself stays strictly ordered.
LEGAL_NEXT: dict[C, set[C]] = {
    C.QUEUED:    {C.RESERVED} | TERMINAL,
    C.RESERVED:  {C.INITIATED} | TERMINAL,
    C.INITIATED: {C.RINGING, C.ANSWERED} | TERMINAL,
    C.RINGING:   {C.ANSWERED} | TERMINAL,
    C.ANSWERED:  {C.CONNECTED} | TERMINAL,
    C.CONNECTED: TERMINAL,
    **{t: set() for t in TERMINAL},
}


def can_apply(current: C, incoming: C) -> tuple[bool, str | None]:
    """Returns (apply?, anomaly_reason)."""
    if RANK[current] >= TERMINAL_RANK:
        return False, "OUT_OF_ORDER"      # terminal absorbs everything
    if RANK[incoming] <= RANK[current]:
        return False, "OUT_OF_ORDER"      # stale or duplicate-by-content
    if incoming not in LEGAL_NEXT[current]:
        return False, "ILLEGAL_TRANSITION"
    return True, None


# agent side effects are keyed to the CALL transition, never to the raw event
AGENT_ON_CALL_STATE: dict[C, A] = {
    C.INITIATED: A.DIALING,
    C.CONNECTED: A.CONNECTED,
    C.COMPLETED: A.WRAP_UP,
    C.FAILED: A.AVAILABLE,
    C.CANCELLED: A.AVAILABLE,
    C.ABANDONED: A.AVAILABLE,
}
