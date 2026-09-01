class ProviderRegistry:
    """Health-aware routing: pick the healthy provider with the best score. An
    open circuit removes a provider from rotation and its score of 0 feeds
    `MetricsSnapshot.provider_health`, which the safety controller reads
    (rule S4) — degradation is visible to pacing before it becomes a wall of
    provider errors."""

    def __init__(self, providers: list):
        self.providers = {p.name: p for p in providers}

    def pick(self):
        candidates = [p for p in self.providers.values() if p.health().healthy]
        if not candidates:
            return None
        return max(candidates, key=lambda p: p.health().score)

    def get(self, name: str):
        return self.providers[name]

    def record(self, name: str, ok: bool) -> None:
        self.providers[name].breaker.record(ok)

    def health_map(self) -> dict[str, float]:
        return {name: p.health().score for name, p in self.providers.items()}
