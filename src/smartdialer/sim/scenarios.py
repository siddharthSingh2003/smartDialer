import math
from collections.abc import Callable
from dataclasses import dataclass, field

from .injectors import AgentMassLogout, ProviderOutage

# Durations here are tuned down from the assignment's illustrative 30-minute
# runs (ARCHITECTURE.md §10.3) so the full scenario suite finishes in a few
# minutes of *wall* time against a real Postgres instance — the virtual clock
# compresses simulated time, not the database round trips each tick makes.
# Pass --duration/--agents on the CLI for a longer, denser run.


@dataclass
class Scenario:
    name: str
    answer_rate: float | Callable[[float], float]
    talk_time_s: float | Callable[[float], float]
    agents: int = 50
    duration_s: float = 300
    mode: str = "PREDICTIVE"
    injections: list = field(default_factory=list)


def _d_answer_rate(t: float) -> float:
    if t < 100:
        return 0.70
    if t < 200:
        return 0.10
    return 0.45


def _d_talk_time(t: float) -> float:
    return 120 + 60 * math.sin(t / 60)


SCENARIOS: dict[str, Scenario] = {
    "A": Scenario("A", 0.20, 120),
    "B": Scenario("B", 0.50, 90),
    "C": Scenario("C", 0.70, 180),
    "D": Scenario("D", answer_rate=_d_answer_rate, talk_time_s=_d_talk_time),
    "E_outage": Scenario("E_outage", 0.50, 90,
                          injections=[ProviderOutage(at=120, dur=60)]),
    "F_agentdrop": Scenario("F_agentdrop", 0.50, 90,
                             injections=[AgentMassLogout(at=120, count=20)]),
    "G_progressive_baseline": Scenario("G_progressive_baseline", 0.50, 90, mode="PROGRESSIVE"),
}
