from dataclasses import asdict, dataclass

from ..config import SafetyLimits
from ..domain.enums import ReasonCode
from ..domain.models import MetricsSnapshot, PacingRequest, SafetyDecision
from .rules import RULES


@dataclass
class SafetyContext:
    limits: SafetyLimits
    cooldown_ticks_left: int = 0

    def trip_cooldown(self) -> None:
        self.cooldown_ticks_left = self.limits.cooldown_ticks_after_breach

    def tick(self) -> None:
        if self.cooldown_ticks_left > 0:
            self.cooldown_ticks_left -= 1


class SafetyController:
    """The only object constructed with the CallAllocator. `pacing_loop.py`
    calls `engine.decide()` then `controller.evaluate_and_execute()`; there is
    no code path from a request to a placed call that does not pass through
    here. See ARCHITECTURE.md §8 for the three-layer non-bypassability proof.
    """

    def __init__(self, limits: SafetyLimits, allocator, decisions_repo, clock):
        self._allocator = allocator          # private: engines never receive this
        self.limits = limits
        self.ctx = SafetyContext(limits=limits)
        self.decisions = decisions_repo
        self.clock = clock

    async def evaluate_and_execute(self, req: PacingRequest, s: MetricsSnapshot,
                                    mode: str = "PREDICTIVE",
                                    sim_tick: int | None = None) -> SafetyDecision:
        approved, reason = req.n, ReasonCode.OK
        applied = []
        for rule in RULES:                    # every rule runs; the MINIMUM wins
            out = rule(req, s, self.ctx)
            if out is None:
                continue
            n, code = out
            applied.append({"rule": rule.__name__, "cap": n, "reason": str(code)})
            if n < approved:
                approved, reason = n, code
        approved = max(0, min(approved, req.n))
        self.ctx.tick()

        await self.decisions.record(
            req.campaign_id, self.clock.now(), req.n, approved, str(reason),
            {**req.rationale, "rules_applied": applied, "snapshot": asdict(s)},
            mode=mode, sim_tick=sim_tick)

        placed = 0
        if approved > 0:
            placed = await self._allocator.allocate_batch(req.campaign_id, approved)
        return SafetyDecision(approved, reason, req.n,
                               {"rules_applied": applied, "placed": placed})

    async def cancel_excess_ringing(self, campaign_id: int, allowed_ringing: int) -> int:
        """Free-cancellation pullback (§12.3), routed through the controller so
        the allocator handle never has to leave this object."""
        return await self._allocator.cancel_excess_ringing(campaign_id, allowed_ringing)
