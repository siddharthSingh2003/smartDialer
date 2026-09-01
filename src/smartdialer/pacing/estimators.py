import math
from collections import deque


def wilson_bounds(phat: float, n: int, z: float) -> tuple[float, float]:
    """One-sided-flavoured Wilson interval, both edges. Pessimistic in either
    direction depending on which edge the caller reads — see §10.4."""
    if n == 0:
        return 0.05, 1.0                      # unknown: paces like progressive, caps at 1
    denom = 1 + z * z / n
    centre = phat + z * z / (2 * n)
    margin = z * math.sqrt(phat * (1 - phat) / n + z * z / (4 * n * n))
    lo = max(0.01, (centre - margin) / denom)
    hi = min(1.0, (centre + margin) / denom)
    return lo, hi


class WilsonLowerBound:
    """Conservative answer-rate estimate. Pessimistic when data is thin — by design."""

    def __init__(self, z: float = 1.2816, window: int = 300):   # z for 90% one-sided
        self.z = z
        self.obs: deque[int] = deque(maxlen=window)

    def record(self, answered: bool) -> None:
        self.obs.append(1 if answered else 0)

    @property
    def n(self) -> int:
        return len(self.obs)

    @property
    def point(self) -> float:
        return (sum(self.obs) / self.n) if self.n else 0.0

    def lower_bound(self) -> float:
        lo, _ = wilson_bounds(self.point, self.n, self.z)
        return lo

    def upper_bound(self) -> float:
        _, hi = wilson_bounds(self.point, self.n, self.z)
        return hi


class AsymmetricEWMA:
    """Falls fast, rises slow. Protects against a 70%->10% answer-rate collapse."""

    def __init__(self, fast=0.30, slow=0.05):
        self.fast_a, self.slow_a = fast, slow
        self.fast = self.slow = None

    def record(self, x: float) -> None:
        self.fast = x if self.fast is None else self.fast_a * x + (1 - self.fast_a) * self.fast
        self.slow = x if self.slow is None else self.slow_a * x + (1 - self.slow_a) * self.slow

    @property
    def value(self) -> float:
        if self.fast is None:
            return 0.0
        return min(self.fast, self.slow)            # asymmetry lives in this min()


class SetupTimeEstimator:
    """Rolling mean of call-setup latency (initiate -> answer), used as the
    Little's Law horizon in predictive.py."""

    def __init__(self, window: int = 200, default_s: float = 3.0):
        self.window = window
        self.default_s = default_s
        self.obs: deque[float] = deque(maxlen=window)

    def record(self, seconds: float) -> None:
        if seconds >= 0:
            self.obs.append(seconds)

    @property
    def mean(self) -> float:
        return (sum(self.obs) / len(self.obs)) if self.obs else self.default_s
