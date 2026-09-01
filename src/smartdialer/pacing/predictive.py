import math

from ..domain.models import MetricsSnapshot, PacingRequest


class PredictivePacing:
    """See ARCHITECTURE.md §10 for the derivation and §10.4 for why the upper
    bound (not the lower bound) is the pessimistic choice for the divisor."""

    mode = "PREDICTIVE"

    def decide(self, s: MetricsSnapshot) -> PacingRequest:
        # Pessimistic about how many of OUR calls will connect (p_hi) — assuming
        # a higher answer rate produces a SMALLER n (n = capacity / p), which is
        # the safe direction. Also pessimistic about capacity already consumed
        # by calls that are already ringing, using the same upper bound.
        p_hi = max(0.01, min(1.0, s.answer_rate_ub))

        freeing = 0.0
        if s.avg_talk_s > 0:
            freeing = s.calls_connected * (s.avg_setup_s / s.avg_talk_s)

        expected_answers = s.calls_ringing * p_hi
        capacity = s.agents_available + freeing - expected_answers
        n = max(0, math.floor(capacity / p_hi))

        return PacingRequest(s.campaign_id, n, {
            "p_answer_lb": round(s.answer_rate_lb, 4),
            "p_answer_ub": round(p_hi, 4),
            "p_answer_point": round(s.answer_rate_point, 4),
            "samples": s.answer_samples,
            "available": s.agents_available,
            "connected": s.calls_connected,
            "ringing": s.calls_ringing,
            "avg_setup_s": round(s.avg_setup_s, 3),
            "avg_talk_s": round(s.avg_talk_s, 2),
            "freeing_soon": round(freeing, 3),
            "expected_answers_from_ringing": round(expected_answers, 3),
            "net_capacity": round(capacity, 3),
            "formula": "floor((available + freeing_soon - ringing*p_ub) / p_ub)",
        })
