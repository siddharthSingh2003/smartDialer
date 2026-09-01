from ..domain.models import MetricsSnapshot, PacingRequest


class ProgressivePacing:
    """1:1 dialing. The whole engine is this subtraction: never place a call
    that doesn't have an agent already committed to it."""

    mode = "PROGRESSIVE"

    def decide(self, s: MetricsSnapshot) -> PacingRequest:
        n = max(0, s.agents_available - s.agent_bound_inflight)
        return PacingRequest(s.campaign_id, n, {
            "rule": "1:1",
            "available": s.agents_available,
            "agent_bound_inflight": s.agent_bound_inflight,
        })
