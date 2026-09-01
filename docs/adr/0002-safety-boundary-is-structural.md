# ADR-0002 — The safety boundary is structural, not procedural

**Context.** The assignment's central requirement is that the predictive
pacing algorithm cannot switch the safety mechanism off, by accident or by a
future developer's shortcut.

**Decision.** `PacingEngine.decide()` takes a `MetricsSnapshot` and returns a
`PacingRequest` — both frozen dataclasses of numbers, with no database handle,
no provider, no allocator reachable from either type. `SafetyController` is
the only object ever constructed with the `CallAllocator`; `pacing_loop.py`
always calls `engine.decide()` then `controller.evaluate_and_execute()`, so
there is no code path from a request to a placed call that skips the
controller. There is no `enabled` flag anywhere — `SafetyLimits.__post_init__`
clamps every threshold to a compiled-in ceiling, so not even a malicious
config file can raise the overdial cap above 2.0 or the abandon budget above
5%. `tests/test_safety_boundary.py` AST-walks every file in `pacing/` and
fails CI if it ever imports `allocator`, `providers`, or `repo`.

**Consequence.** Three independent layers back the same guarantee: type-level
(no capability in the data), wiring-level (only one constructor call holds the
allocator), and test-level (an AST assertion that stays true even if someone
tries to route around the first two).

**What it makes harder.** The pacing engine cannot do adaptive lookups
mid-decision — everything it needs must already be in the snapshot, which
pushes complexity into `repo/metrics.py` instead of the engine.

**What would change my mind.** Nothing — this is the assignment's central
requirement, and weakening it to make the engine's job easier would be
solving the wrong problem.
