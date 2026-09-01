import pytest

from smartdialer.clock import RealClock
from smartdialer.providers.mock_a import MockProviderA

pytestmark = pytest.mark.asyncio


class RecordingSink:
    def __init__(self):
        self.events: list[tuple[str, str]] = []

    async def deliver(self, provider, event_id, provider_call_id, event_type, ts, payload):
        self.events.append((provider_call_id, event_type))


async def test_held_reorder_buffer_is_scoped_per_call_not_shared():
    """Regression test: a single shared 'held' slot for the reorder-pairing
    buffer let an event genuinely emitted for one call get paired with, and
    flushed under, a DIFFERENT concurrently in-flight call's pcid — handing
    that other call a stray event it never actually received. Found by
    watching a live worker: a call with answered_at/connected_at set that
    nonetheless ended FAILED a few milliseconds later, from an event that
    actually belonged to a different call placed around the same time. See
    providers/mock_a.py::_emit and the README 'Implementation notes'
    section."""
    clock = RealClock()
    sink = RecordingSink()
    provider = MockProviderA(clock, sink, seed=1)
    provider.reorder_rate = 1.0   # force pairing/swap on every emit

    await provider._emit("PCID-A", "ringing")
    await provider._emit("PCID-B", "ringing")
    await provider._flush("PCID-A")
    await provider._flush("PCID-B")

    pcids_seen = {pcid for pcid, _etype in sink.events}
    assert pcids_seen == {"PCID-A", "PCID-B"}
    assert len(sink.events) == 2
