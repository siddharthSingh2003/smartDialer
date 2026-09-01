import asyncio
import heapq
import time
from contextlib import asynccontextmanager
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float: ...
    async def sleep(self, seconds: float) -> None: ...


class RealClock:
    def now(self) -> float:
        return time.time()

    async def sleep(self, s: float) -> None:
        await asyncio.sleep(s)


class VirtualClock:
    """Discrete-event clock. Time advances only when every task is waiting.

    That guarantee needs one piece of cooperation from a driven task that
    does real I/O between clock waits (repo/DB calls, not just other
    coroutines): wrap its per-iteration body in `async with clock.critical():`
    (see worker/pacing_loop.py's caller in sim/runner.py). Without it,
    `advance_to_next` can only tell "nothing is waiting on the clock right
    now" — which is also true, misleadingly, while that task is busy with
    real I/O and simply hasn't reached its next `clock.sleep()` yet. If some
    *other*, unrelated task (e.g. a background call's 90-second talk timer)
    already has a wait registered, `advance_to_next` would jump straight to
    that distant deadline, skipping straight past where the busy task's own
    near-term wait would have landed. `critical()` tells it to hold off
    resolving anything at all until the busy task has settled back into a
    wait (or finished) — see the invariant in `advance_to_next` below.
    """

    def __init__(self, start: float = 0.0):
        self._now = start
        self._waiters: list[tuple[float, int, asyncio.Future]] = []
        self._seq = 0
        self._critical_depth = 0

    def now(self) -> float:
        return self._now

    async def sleep(self, seconds: float) -> None:
        fut = asyncio.get_running_loop().create_future()
        self._seq += 1
        heapq.heappush(self._waiters, (self._now + seconds, self._seq, fut))
        await fut

    @asynccontextmanager
    async def critical(self):
        """Marks "a driven task is doing real work, not currently blocked on
        this clock" — see the class docstring."""
        self._critical_depth += 1
        try:
            yield
        finally:
            self._critical_depth -= 1

    async def advance_to_next(self) -> bool:
        """Let all currently-runnable tasks finish, then jump to the next
        deadline.

        Two distinct phases, deliberately treated differently:

        1. While `critical()` is held, just wait for it to end — full stop,
           no timeout. We *know* something is legitimately busy (real I/O
           under contention can legitimately take a while: 50 concurrent
           allocations retrying past a deadlock, a slow query), so there is
           no ambiguity to resolve with a budget, and no bound would be safe
           to pick — cutting this phase short would cancel a still-in-progress
           critical task out from under itself (see sim/runner.py's
           `task.cancel()` right after this returns).
        2. Once nothing is critical, a registered waiter might still not
           exist yet purely because the task that will register it hasn't
           reached that point (real I/O between two clock waits). That IS
           genuinely ambiguous — "nothing yet" and "nothing ever" look
           identical — so it gets a generous but bounded retry: first for
           free, then with a tiny real delay, until a waiter appears or the
           budget runs out.
        """
        while self._critical_depth > 0:
            await asyncio.sleep(0.001)

        if not self._waiters:
            for _ in range(500):
                await asyncio.sleep(0)
                if self._waiters:
                    break
            else:
                # Bounded by wall-clock time, not iteration count: Windows'
                # default timer resolution is often ~15ms, not 1ms, so a
                # fixed iteration count of tiny sleeps can cost far more real
                # time there than on Linux/macOS for the same nominal budget.
                deadline = time.monotonic() + 2.0
                while time.monotonic() < deadline:
                    await asyncio.sleep(0.001)
                    if self._waiters:
                        break
                else:
                    return False

        deadline, _, _ = self._waiters[0]
        self._now = deadline
        while self._waiters and self._waiters[0][0] <= self._now:
            _, _, fut = heapq.heappop(self._waiters)
            if not fut.done():
                fut.set_result(None)
        await asyncio.sleep(0)
        return True

    async def run_until(self, end: float) -> None:
        while self._now < end and await self.advance_to_next():
            pass
