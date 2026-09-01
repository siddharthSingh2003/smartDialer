import asyncio
import random
from uuid import uuid4

from .base import (
    CallRequest,
    HealthSnapshot,
    ProviderError,
    ProviderTimeout,
    map_provider_event,
)
from .breaker import CircuitBreaker


class _MockProviderBase:
    """Shared mock provider behaviour: setup latency, failure/timeout draws,
    an injectable outage window, and duplicate/out-of-order webhook delivery.
    MockProviderA and MockProviderB are the same class with different dials —
    see ARCHITECTURE.md §5 for the behaviour matrix each is tuned to."""

    name = "mock"

    def __init__(self, clock, sink, seed: int = 0,
                 setup_range: tuple[float, float] = (0.2, 0.4),
                 hard_fail_rate: float = 0.02, timeout_rate: float = 0.0,
                 dup_rate: float = 0.0, reorder_rate: float = 0.0,
                 ring_before_answer_s: float = 0.3):
        self.clock = clock
        self.sink = sink
        self.rng = random.Random(seed)
        self.setup_range = setup_range
        self.hard_fail_rate = hard_fail_rate
        self.timeout_rate = timeout_rate
        self.dup_rate = dup_rate
        self.reorder_rate = reorder_rate
        self.ring_before_answer_s = ring_before_answer_s

        self.breaker = CircuitBreaker(clock)
        self.outage_until = -1.0
        self._held: dict[str, dict] = {}     # keyed by pcid — see _emit's docstring
        self.tasks: set[asyncio.Task] = set()

        # Scenario-driven; the sim/CLI wiring overrides these per campaign.
        self.answer_probability = lambda: 0.5
        self.talk_time_sampler = lambda: 90.0

    def health(self) -> HealthSnapshot:
        return HealthSnapshot(
            name=self.name, healthy=self.breaker.allow(),
            error_rate=self.breaker.error_rate, p95_setup_s=self.setup_range[1],
            open_circuit=self.breaker.state == "open")

    def trigger_outage(self, duration_s: float) -> None:
        self.outage_until = self.clock.now() + duration_s

    async def place_call(self, req: CallRequest) -> str:
        # Deliberately not a clock.sleep(): this models the provider-API
        # accept latency, which happens before INITIATED is even recorded and
        # so never feeds avg_setup_s (that's measured from INITIATED to
        # ANSWERED — the ringing delay in _run_lifecycle below, which does
        # use the clock). Registering it as a virtual-time wait would put a
        # cluster of N concurrent waits into the clock mid-tick every time
        # allocate_batch places a batch, racing against the tick driver's own
        # cadence wait for no benefit this metric ever observes.
        await asyncio.sleep(0)
        if self.clock.now() < self.outage_until:
            raise ProviderTimeout(self.name)
        if self.rng.random() < self.timeout_rate:
            raise ProviderTimeout(self.name)
        if self.rng.random() < self.hard_fail_rate:
            raise ProviderError("congestion")
        pcid = f"{self.name.upper()}-{uuid4().hex[:12]}"
        task = asyncio.ensure_future(self._run_lifecycle(req, pcid))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return pcid

    async def cancel(self, provider_call_id: str) -> None:
        return None   # mock: nothing to tear down server-side

    async def _run_lifecycle(self, req: CallRequest, pcid: str) -> None:
        await self._emit(pcid, "ringing")
        await self.clock.sleep(self.ring_before_answer_s)
        answered = self.rng.random() < self.answer_probability()
        if not answered:
            await self._emit(pcid, "no-answer")
            await self._flush(pcid)
            return
        await self._emit(pcid, "answered")
        talk_s = max(1.0, self.talk_time_sampler())
        await self.clock.sleep(talk_s)
        await self._emit(pcid, "completed")
        await self._flush(pcid)

    async def _emit(self, pcid: str, raw_event_type: str) -> None:
        """Hold-and-pair reordering is scoped per call (keyed by pcid), not
        shared across every concurrently in-flight call on this provider
        instance. A single shared "held" slot would let an event that
        actually belongs to one call get paired with, and flushed under, a
        completely different call's pcid the moment two calls are in flight
        at once — corrupting that other call's event stream (e.g. handing it
        a stray FAILED that was never really its own)."""
        etype = map_provider_event(raw_event_type)
        batch = [{"event_id": str(uuid4()), "etype": etype,
                   "ts": self.clock.now(), "raw": raw_event_type}]
        if self.rng.random() < self.dup_rate:
            batch.append(dict(batch[0]))          # same event_id: ledger dedups it

        for ev in batch:
            held = self._held.get(pcid)
            if held is None:
                self._held[pcid] = ev
                continue
            pair = [held, ev]
            if self.rng.random() < self.reorder_rate:
                pair.reverse()
            del self._held[pcid]
            for p in pair:
                await self._send(pcid, p)

    async def _flush(self, pcid: str) -> None:
        held = self._held.pop(pcid, None)
        if held is not None:
            await self._send(pcid, held)

    async def _send(self, pcid: str, ev: dict) -> None:
        await self.sink.deliver(self.name, ev["event_id"], pcid, ev["etype"],
                                 ev["ts"], {"raw": ev["raw"]})


class MockProviderA(_MockProviderBase):
    """Fast and reliable."""
    name = "mock_a"

    def __init__(self, clock, sink, seed: int = 0):
        super().__init__(clock, sink, seed=seed, setup_range=(0.2, 0.4),
                          hard_fail_rate=0.02, timeout_rate=0.0,
                          dup_rate=0.0, reorder_rate=0.0)
