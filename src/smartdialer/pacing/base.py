from typing import Protocol

from ..domain.models import MetricsSnapshot, PacingRequest


class PacingEngine(Protocol):
    mode: str

    def decide(self, s: MetricsSnapshot) -> PacingRequest: ...
