import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class SafetyLimits:
    """Hard floors. There is deliberately no `enabled` flag anywhere in this class."""
    max_overdial_ratio: float = 1.5      # in-flight agent-bound calls per available agent
    abs_max_overdial_ratio: float = 2.0  # ceiling nobody can exceed
    max_abandon_rate: float = 0.03
    min_samples_for_predictive: int = 30
    ringing_hard_cap: int = 500
    cooldown_ticks_after_breach: int = 40   # 40 * 250ms = 10s of forced progressive

    def __post_init__(self) -> None:
        # clamp: config cannot loosen safety beyond the compiled-in ceiling
        object.__setattr__(self, "max_overdial_ratio",
                            min(self.max_overdial_ratio, self.abs_max_overdial_ratio))
        object.__setattr__(self, "max_abandon_rate", min(self.max_abandon_rate, 0.05))


@dataclass(frozen=True)
class Settings:
    database_url: str = field(default_factory=lambda: os.environ.get(
        "DATABASE_URL", "postgresql://dialer:dialer@localhost:5432/dialer"))
    tick_ms: int = field(default_factory=lambda: int(os.environ.get("TICK_MS", "250")))
    reaper_hz: float = field(default_factory=lambda: float(os.environ.get("REAPER_HZ", "1.0")))
    agent_reserve_lease_s: int = field(
        default_factory=lambda: int(os.environ.get("AGENT_RESERVE_LEASE_S", "30")))
    call_setup_lease_s: int = field(
        default_factory=lambda: int(os.environ.get("CALL_SETUP_LEASE_S", "45")))
    wrapup_s: int = field(default_factory=lambda: int(os.environ.get("WRAPUP_S", "8")))
    caller_id: str = "+10000000000"
    safety: SafetyLimits = field(default_factory=SafetyLimits)
