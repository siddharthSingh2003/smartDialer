import time

import pytest

from smartdialer.clock import VirtualClock

pytestmark = pytest.mark.asyncio


async def test_virtual_clock_resolves_in_deadline_order_fast():
    clock = VirtualClock()
    order: list[str] = []

    async def waiter(name: str, delay: float):
        await clock.sleep(delay)
        order.append(name)

    import asyncio
    start = time.time()
    task_c = asyncio.ensure_future(waiter("c", 30))
    task_a = asyncio.ensure_future(waiter("a", 5))
    task_b = asyncio.ensure_future(waiter("b", 15))
    # end=30 exactly matches the last waiter's deadline: run_until's while
    # condition goes false the instant it resolves, without needing a further
    # "is there anything else" round (which is real-time-bounded, not free —
    # see advance_to_next's give-up tier — and would legitimately cost that
    # budget here, since nothing else is registered after c resolves).
    await clock.run_until(30)
    await asyncio.gather(task_a, task_b, task_c)
    elapsed = time.time() - start

    assert order == ["a", "b", "c"]
    assert elapsed < 0.05
    assert clock.now() == 30
