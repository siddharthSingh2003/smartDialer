from collections import deque


class CircuitBreaker:
    """closed -> open on error-rate breach -> half-open probe -> closed."""

    def __init__(self, clock, window_s: float = 10.0, threshold: float = 0.25,
                 min_calls: int = 10, cooldown_s: float = 15.0, half_open_probes: int = 3):
        self.clock = clock
        self.window_s = window_s
        self.threshold = threshold
        self.min_calls = min_calls
        self.cooldown_s = cooldown_s
        self.half_open_probes = half_open_probes
        self.state = "closed"
        self.opened_at = 0.0
        self.probes = 0
        self._events: deque[tuple[float, bool]] = deque()

    def _trim(self, now: float) -> None:
        while self._events and now - self._events[0][0] > self.window_s:
            self._events.popleft()

    def _open(self, now: float) -> None:
        self.state = "open"
        self.opened_at = now
        self.probes = 0

    def record(self, ok: bool) -> None:
        now = self.clock.now()
        self._events.append((now, ok))
        self._trim(now)

        if self.state == "half_open":
            self.probes += 1
            if not ok:
                self._open(now)
            elif self.probes >= self.half_open_probes:
                self.state = "closed"
                self._events.clear()
            return

        if self.state == "closed" and len(self._events) >= self.min_calls:
            errors = sum(1 for _, o in self._events if not o)
            if errors / len(self._events) >= self.threshold:
                self._open(now)

    def allow(self) -> bool:
        if self.state == "open":
            if self.clock.now() >= self.opened_at + self.cooldown_s:
                self.state = "half_open"
                self.probes = 0
                return True                     # let the first probe through
            return False
        if self.state == "half_open":
            return self.probes < self.half_open_probes
        return True

    @property
    def error_rate(self) -> float:
        if not self._events:
            return 0.0
        errors = sum(1 for _, o in self._events if not o)
        return errors / len(self._events)
