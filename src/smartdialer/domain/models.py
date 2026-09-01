from dataclasses import dataclass, field

from .enums import ReasonCode


@dataclass(frozen=True)
class MetricsSnapshot:
    campaign_id: int
    ts: float                    # clock.now() — virtual or real
    agents_available: int
    agents_reserved: int
    agents_dialing: int
    agents_connected: int
    agents_wrapup: int
    calls_ringing: int           # INITIATED + RINGING
    calls_connected: int
    answer_rate_lb: float        # Wilson lower bound, conservative
    answer_rate_ub: float        # Wilson upper bound, used as the pessimistic divisor
    answer_rate_point: float
    answer_samples: int
    avg_setup_s: float
    avg_talk_s: float
    abandon_rate_5m: float
    campaign_active: bool = True
    provider_health: dict = field(default_factory=dict)  # name -> 0..1

    @property
    def agent_bound_inflight(self) -> int:
        return self.agents_reserved + self.agents_dialing

    @property
    def agents_staffed(self) -> int:
        """Total logged-in agents (every non-OFFLINE/PAUSED state). Used as
        the overdial ceiling's base instead of `agents_available` alone:
        `agents_available` shrinks the moment agents legitimately go busy
        dialing calls the system itself just placed, which would make a
        ceiling built on it shrink in lockstep with normal utilization —
        self-reinforcing churn, not a safety response to an actual capacity
        drop. Staffing only changes when agents log in/out or wrap up, which
        is the thing S6 and the pullback sweep are actually meant to react
        to."""
        return (self.agents_available + self.agents_reserved + self.agents_dialing
                + self.agents_connected + self.agents_wrapup)


@dataclass(frozen=True)
class PacingRequest:
    """Pure value object. Carries NO capability to act."""
    campaign_id: int
    n: int
    rationale: dict                 # every input + intermediate, logged verbatim


@dataclass(frozen=True)
class SafetyDecision:
    approved: int
    reason: ReasonCode
    requested: int
    evaluated: dict
