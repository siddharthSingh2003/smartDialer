from .mock_a import _MockProviderBase


class MockProviderB(_MockProviderBase):
    """Slow, unreliable, duplicates events, occasionally reorders them."""
    name = "mock_b"

    def __init__(self, clock, sink, seed: int = 1):
        super().__init__(clock, sink, seed=seed, setup_range=(0.8, 2.5),
                          hard_fail_rate=0.15, timeout_rate=0.08,
                          dup_rate=0.20, reorder_rate=0.25)
