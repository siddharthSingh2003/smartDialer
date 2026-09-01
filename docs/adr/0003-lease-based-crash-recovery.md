# ADR-0003 — Lease-based crash recovery instead of distributed locks or heartbeats

**Context.** A worker can die at any point between reserving an agent and
recording a provider's response, and the system has to recover without a
central "is this worker alive" service.

**Decision.** Every claim — an agent reservation, a borrower lock, an
in-flight call — carries an expiry (`lease_expires_at`). A 1 Hz reaper
reconciles anything past it: it fails orphaned calls first, then reclaims
`RESERVED` agents (deliberately *not* `DIALING` ones — see the "least
confident" answer in ARCHITECTURE.md §17), then reschedules the orphaned
borrower with backoff. `UNIQUE(idempotency_key)` makes a post-crash retry of
the same attempt a rejected insert, not a double dial.

**Consequence.** Recovery does not depend on any process detecting another
process's death — it depends only on wall-clock time passing, which Postgres
already tracks for us.

**What it makes harder.** Recovery latency is bounded by the lease duration,
not instant — worst case is `call_setup_lease_s` (45s) plus one reaper tick.
Leases that are too short risk reaping still-live work; too long delays
recovery. Lease length is a real, tuned parameter, not an arbitrary constant.

**What would change my mind.** A requirement for sub-second failover, which
would need heartbeats plus fencing tokens instead of a lease-and-sweep model.
