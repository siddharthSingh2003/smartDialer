from smartdialer.providers.breaker import CircuitBreaker


class FakeClock:
    def __init__(self, t: float = 1000.0):
        self.t = t

    def now(self) -> float:
        return self.t


def test_breaker_opens_then_half_opens_then_closes():
    clock = FakeClock()
    b = CircuitBreaker(clock, window_s=10.0, threshold=0.25, min_calls=10,
                        cooldown_s=15.0, half_open_probes=3)

    for _ in range(20):
        b.record(False)
    assert b.state == "open"
    assert b.allow() is False

    clock.t += 16          # advance past the cooldown
    assert b.allow() is True
    assert b.state == "half_open"

    b.record(True)
    b.record(True)
    b.record(True)
    assert b.state == "closed"
    assert b.allow() is True


def test_breaker_reopens_on_half_open_failure():
    clock = FakeClock()
    b = CircuitBreaker(clock, min_calls=5, threshold=0.25, cooldown_s=10.0)
    for _ in range(10):
        b.record(False)
    assert b.state == "open"

    clock.t += 11
    assert b.allow() is True
    assert b.state == "half_open"

    b.record(False)          # a single failed probe re-opens immediately
    assert b.state == "open"
    assert b.allow() is False


def test_breaker_stays_closed_below_threshold():
    clock = FakeClock()
    b = CircuitBreaker(clock, min_calls=10, threshold=0.25)
    # 16 successes then 4 failures: every prefix ratio from min_calls onward
    # stays below 25% (worst case 4/20 = 20% at the very end), unlike an
    # interleaved pattern where an early run of failures can transiently
    # cross the threshold even though the eventual aggregate would not.
    for _ in range(16):
        b.record(True)
    for _ in range(4):
        b.record(False)
    assert b.state == "closed"
    assert b.allow() is True
