# ADR-0005 — Virtual clock injected everywhere

**Context.** Demonstrating predictive dialing needs 20-30 minutes of simulated
campaign behaviour, and the test suite needs deterministic timing for lease
expiry, circuit-breaker cooldowns, and cooldown-tick counting.

**Decision.** `clock.py` defines a `Clock` protocol with `now()` and `sleep()`;
`RealClock` wraps `time.time()`/`asyncio.sleep()` for production, `VirtualClock`
is a discrete-event clock that advances only when every waiting task is
actually blocked on it, jumping straight to the next deadline. Nothing in the
codebase calls `time.time()` or `asyncio.sleep()` directly — every duration,
lease, and cooldown flows through an injected clock.

**Consequence.** A 30-minute simulated campaign runs in a couple of seconds of
wall time, tests are deterministic under a fixed seed, and failure scenarios
are exactly replayable. One caveat had to be handled explicitly: lease
timestamps are written to Postgres as real `TIMESTAMPTZ` values via
`to_timestamp(clock.now() + lease_s)`, so any `VirtualClock` used against the
real database must be seeded at `time.time()`, not `0.0` — otherwise every
lease looks already-expired to a reaper that compares against Postgres' own
`now()`. See the seeding note in `sim/runner.py` and `tests/conftest.py`.

**What it makes harder.** Every new duration in the codebase has to remember
to go through the clock; a lint-style discipline (grep for bare
`asyncio.sleep` outside `clock.py`) is the enforcement mechanism, since a
compile-time guarantee isn't available in Python.

**What would change my mind.** Nothing changes this for a system whose whole
value proposition depends on being demonstrable in simulation.
