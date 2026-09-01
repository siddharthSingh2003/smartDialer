from smartdialer.domain.models import MetricsSnapshot
from smartdialer.pacing.predictive import PredictivePacing
from smartdialer.pacing.progressive import ProgressivePacing


def _snap(**over) -> MetricsSnapshot:
    base = dict(campaign_id=1, ts=0.0, agents_available=10, agents_reserved=0,
                agents_dialing=0, agents_connected=0, agents_wrapup=0, calls_ringing=6,
                calls_connected=40, answer_rate_lb=0.15, answer_rate_ub=0.25,
                answer_rate_point=0.20, answer_samples=200, avg_setup_s=3.0, avg_talk_s=120.0,
                abandon_rate_5m=0.0, campaign_active=True, provider_health={})
    base.update(over)
    return MetricsSnapshot(**base)


def test_predictive_worked_example_matches_architecture_doc_section_10_3():
    """ARCHITECTURE.md §10.3: A=10, C=40, R=6, Ts=3s, Tt=120s.
    freeing_soon = 40*(3/120) = 1.0
    expected_from_ringing = 6*0.25 = 1.5
    capacity = 10 + 1.0 - 1.5 = 9.5
    n = floor(9.5 / 0.25) = 38
    (§10.4's fix uses the upper bound as the pessimistic divisor; the example
    numbers are unchanged, only the field that feeds them is.)"""
    engine = PredictivePacing()
    req = engine.decide(_snap())
    assert req.n == 38
    assert req.rationale["freeing_soon"] == 1.0
    assert req.rationale["expected_answers_from_ringing"] == 1.5
    assert req.rationale["net_capacity"] == 9.5


def test_predictive_never_goes_negative():
    engine = PredictivePacing()
    req = engine.decide(_snap(agents_available=0, calls_connected=0, calls_ringing=50,
                               answer_rate_ub=0.9))
    assert req.n == 0


def test_predictive_higher_answer_rate_requests_fewer_calls():
    """The §10.4 inversion: n = capacity / p, so a HIGHER p produces a SMALLER
    n. Using the upper bound as the divisor is the pessimistic (safe)
    direction — it assumes more of our calls will connect than they might."""
    engine = PredictivePacing()
    low_p = engine.decide(_snap(answer_rate_ub=0.20)).n
    high_p = engine.decide(_snap(answer_rate_ub=0.60)).n
    assert high_p < low_p


def test_progressive_is_available_minus_agent_bound_inflight():
    engine = ProgressivePacing()
    req = engine.decide(_snap(agents_available=50, agents_reserved=3, agents_dialing=2))
    assert req.n == 45
