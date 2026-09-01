from smartdialer.config import SafetyLimits
from smartdialer.domain.enums import ReasonCode
from smartdialer.domain.models import MetricsSnapshot, PacingRequest
from smartdialer.safety.controller import SafetyController


class StubAllocator:
    def __init__(self):
        self.calls: list[tuple[int, int]] = []

    async def allocate_batch(self, campaign_id: int, n: int) -> int:
        self.calls.append((campaign_id, n))
        return n

    async def cancel_excess_ringing(self, campaign_id: int, allowed: int) -> int:
        return 0


def _snapshot(campaign_id: int, **over) -> MetricsSnapshot:
    base = dict(campaign_id=campaign_id, ts=0.0, agents_available=10, agents_reserved=0,
                agents_dialing=0, agents_connected=0, agents_wrapup=0, calls_ringing=0,
                calls_connected=0, answer_rate_lb=0.3, answer_rate_ub=0.5, answer_rate_point=0.4,
                answer_samples=100, avg_setup_s=3.0, avg_talk_s=90.0, abandon_rate_5m=0.0,
                campaign_active=True, provider_health={"mock_a": 1.0})
    base.update(over)
    return MetricsSnapshot(**base)


async def test_no_agents_rejects_everything(repos, campaign_id, clock):
    """S1, I3: available=0 -> approved 0 regardless of request size."""
    controller = SafetyController(SafetyLimits(), StubAllocator(), repos.decisions, clock)
    decision = await controller.evaluate_and_execute(
        PacingRequest(campaign_id, 20, {}), _snapshot(campaign_id, agents_available=0))
    assert decision.approved == 0
    assert decision.reason == ReasonCode.NO_AGENTS


async def test_campaign_paused_rejects_everything(repos, campaign_id, clock):
    controller = SafetyController(SafetyLimits(), StubAllocator(), repos.decisions, clock)
    decision = await controller.evaluate_and_execute(
        PacingRequest(campaign_id, 5, {}), _snapshot(campaign_id, campaign_active=False))
    assert decision.approved == 0
    assert decision.reason == ReasonCode.CAMPAIGN_PAUSED


async def test_insufficient_samples_forces_progressive_cap(repos, campaign_id, clock):
    limits = SafetyLimits(min_samples_for_predictive=30)
    controller = SafetyController(limits, StubAllocator(), repos.decisions, clock)
    snap = _snapshot(campaign_id, agents_available=10, agents_reserved=2, answer_samples=5)
    decision = await controller.evaluate_and_execute(PacingRequest(campaign_id, 50, {}), snap)
    assert decision.reason == ReasonCode.INSUFFICIENT_SAMPLES
    assert decision.approved == 8   # 10 available - 2 already agent-bound


async def test_abandon_budget_breach_trips_cooldown_for_40_ticks(repos, campaign_id, clock):
    limits = SafetyLimits(max_abandon_rate=0.03, cooldown_ticks_after_breach=40)
    allocator = StubAllocator()
    controller = SafetyController(limits, allocator, repos.decisions, clock)

    breach_snap = _snapshot(campaign_id, abandon_rate_5m=0.09)
    decision = await controller.evaluate_and_execute(PacingRequest(campaign_id, 50, {}), breach_snap)
    assert decision.reason == ReasonCode.ABANDON_BUDGET_BREACH
    assert decision.approved == 10   # progressive fallback: available - inflight

    # even with abandonment back to zero, the next 39 ticks stay forced-progressive
    calm_snap = _snapshot(campaign_id, abandon_rate_5m=0.0)
    for _ in range(39):
        decision = await controller.evaluate_and_execute(PacingRequest(campaign_id, 50, {}), calm_snap)
        assert decision.reason == ReasonCode.COOLDOWN_FORCED_PROGRESSIVE

    # the 41st tick (1 breach tick + 39 cooldown ticks already consumed) is free again
    decision = await controller.evaluate_and_execute(PacingRequest(campaign_id, 5, {}), calm_snap)
    assert decision.reason == ReasonCode.OK
    assert decision.approved == 5


async def test_provider_degraded_scales_or_zeroes(repos, campaign_id, clock):
    controller = SafetyController(SafetyLimits(), StubAllocator(), repos.decisions, clock)
    all_down = _snapshot(campaign_id, provider_health={"mock_a": 0.0, "mock_b": 0.0})
    decision = await controller.evaluate_and_execute(PacingRequest(campaign_id, 20, {}), all_down)
    assert decision.approved == 0
    assert decision.reason == ReasonCode.PROVIDER_DEGRADED

    half_down = _snapshot(campaign_id, provider_health={"mock_a": 0.4})
    decision = await controller.evaluate_and_execute(PacingRequest(campaign_id, 20, {}), half_down)
    assert decision.reason == ReasonCode.PROVIDER_DEGRADED
    assert decision.approved == 8


async def test_overdial_cap_limits_predictive_burst(repos, campaign_id, clock):
    """The §10.3 worked example: predictive requests far more than the
    overdial ceiling allows; the controller cuts it down hard."""
    limits = SafetyLimits(max_overdial_ratio=1.5)
    controller = SafetyController(limits, StubAllocator(), repos.decisions, clock)
    snap = _snapshot(campaign_id, agents_available=10, agents_reserved=3, agents_dialing=3,
                      calls_ringing=6, answer_samples=100)
    decision = await controller.evaluate_and_execute(PacingRequest(campaign_id, 38, {}), snap)
    assert decision.reason == ReasonCode.OVERDIAL_CAP
    # staffed = 10 available + 3 reserved + 3 dialing = 16; ceiling = floor(16*1.5) = 24
    # allowed = 24 - agent_bound_inflight(6) - ringing(6) = 12
    assert decision.approved == 12


async def test_ringing_hard_cap(repos, campaign_id, clock):
    limits = SafetyLimits(ringing_hard_cap=10)
    controller = SafetyController(limits, StubAllocator(), repos.decisions, clock)
    snap = _snapshot(campaign_id, agents_available=100, calls_ringing=8)
    decision = await controller.evaluate_and_execute(PacingRequest(campaign_id, 20, {}), snap)
    assert decision.reason == ReasonCode.RINGING_HARD_CAP
    assert decision.approved == 2


def test_config_cannot_loosen_safety_beyond_compiled_ceiling():
    """I10: there is no `enabled` flag; a malicious/over-eager config is
    clamped in __post_init__, not merely validated."""
    limits = SafetyLimits(max_overdial_ratio=99.0, max_abandon_rate=0.5)
    assert limits.max_overdial_ratio == limits.abs_max_overdial_ratio == 2.0
    assert limits.max_abandon_rate == 0.05
