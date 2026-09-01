from ..domain.enums import ReasonCode


def s0_campaign_active(req, s, ctx):
    return (0, ReasonCode.CAMPAIGN_PAUSED) if not s.campaign_active else None


def s1_no_agents(req, s, ctx):
    return (0, ReasonCode.NO_AGENTS) if s.agents_available <= 0 else None


def s2_cooldown(req, s, ctx):
    if ctx.cooldown_ticks_left > 0:
        cap = max(0, s.agents_available - s.agent_bound_inflight)   # progressive fallback
        return (min(req.n, cap), ReasonCode.COOLDOWN_FORCED_PROGRESSIVE)
    return None


def s3_insufficient_samples(req, s, ctx):
    if s.answer_samples < ctx.limits.min_samples_for_predictive:
        cap = max(0, s.agents_available - s.agent_bound_inflight)
        return (min(req.n, cap), ReasonCode.INSUFFICIENT_SAMPLES)
    return None


def s4_provider_health(req, s, ctx):
    best = max(s.provider_health.values(), default=1.0)
    if best <= 0.0:
        return (0, ReasonCode.PROVIDER_DEGRADED)
    if best < 0.6:
        return (min(req.n, int(req.n * best)), ReasonCode.PROVIDER_DEGRADED)
    return None


def s5_abandon_budget(req, s, ctx):
    if s.abandon_rate_5m > ctx.limits.max_abandon_rate:
        ctx.trip_cooldown()                       # forces progressive for N ticks
        cap = max(0, s.agents_available - s.agent_bound_inflight)
        return (min(req.n, cap), ReasonCode.ABANDON_BUDGET_BREACH)
    return None


def s6_overdial_cap(req, s, ctx):
    # Ceiling is built on total staffed agents, not currently-idle ones:
    # agents_available shrinks the moment agents go busy dialing calls this
    # system itself just placed, which would make a ceiling built on it alone
    # shrink in lockstep with ordinary utilization instead of reacting to an
    # actual drop in capacity (agents logging out). See
    # MetricsSnapshot.agents_staffed.
    ceiling = int(s.agents_staffed * ctx.limits.max_overdial_ratio)
    allowed = max(0, ceiling - s.agent_bound_inflight - s.calls_ringing)
    if req.n > allowed:
        return (allowed, ReasonCode.OVERDIAL_CAP)
    return None


def s7_ringing_hard_cap(req, s, ctx):
    allowed = max(0, ctx.limits.ringing_hard_cap - s.calls_ringing)
    if req.n > allowed:
        return (allowed, ReasonCode.RINGING_HARD_CAP)
    return None


RULES = [s0_campaign_active, s1_no_agents, s2_cooldown, s3_insufficient_samples,
         s4_provider_health, s5_abandon_budget, s6_overdial_cap, s7_ringing_hard_cap]

# NOTE: the non-bypassability guarantee (I9) applies to `pacing/*.py`, not to
# this module — test_safety_boundary.py AST-walks the pacing package and
# asserts it cannot import allocator/providers/repo. This module is part of
# the safety layer and is allowed to know about the outcome of a decision
# (agents_available, abandon rate, ...); it still never imports allocator,
# providers, or repo itself — only controller.py does, deliberately, because
# it is the one object the assignment requires to own the allocator handle.
