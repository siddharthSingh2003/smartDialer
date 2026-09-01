# SmartDialer

Progressive + predictive outbound dialer with a non-bypassable safety
boundary. Built end to end from [ARCHITECTURE.md](ARCHITECTURE.md) — that
document is the full design rationale; this README is the "how to run it."

## Non-goals

- No real-time media handling, no SIP, no audio.
- No ML model. A statistical estimator with a confidence bound (Wilson
  interval) is deliberate, not a shortcut — see ADR-0006.
- No horizontal-scale test at 10,000 agents. §15 of the architecture doc
  reasons about the bottleneck chain instead of pretending to demo it.
- No UI. CSV + matplotlib is the simulation deliverable.

## Quick start

```bash
cp .env.example .env
docker compose up -d                 # postgres
# apply migrations (no local psql needed — runs inside the container)
docker compose exec -T db psql -U dialer -d dialer -f - < migrations/001_init.sql
docker compose exec -T db psql -U dialer -d dialer -f - < migrations/002_counters.sql

python -m venv .venv && .venv/Scripts/pip install -e ".[dev]"   # Windows
# source .venv/bin/activate && pip install -e ".[dev]"          # macOS/Linux
# or: pip install -r requirements-dev.txt (plain requirements.txt files,
# pinned to the exact versions this project was built and tested against —
# pyproject.toml above is still the canonical spec)

.venv/Scripts/python -m smartdialer.cli seed --agents 100 --borrowers 5000
.venv/Scripts/python -m smartdialer.cli api &                   # webhook receiver on :8000
.venv/Scripts/python -m smartdialer.cli worker --campaign-id 1  # run 2-3 for the distributed demo
```

A `Makefile` is included mirroring these steps (`make db`, `make seed`, `make
run`, `make api`, `make sim`, `make test`, `make load`) for environments that
have `make`; this repo was built on Windows without it, so the commands above
are the ones actually exercised.

## Run the tests

```bash
DATABASE_URL=postgresql://dialer:dialer@localhost:5432/dialer .venv/Scripts/python -m pytest -q
```

Tests run against the real dockerized Postgres, not a mock — the concurrency
invariants (I1, I2, I5-I8) are only meaningful if they're proven against the
actual lock/CAS semantics they depend on. `tests/conftest.py` truncates all
tables before each test for isolation.

## Run the simulations

```bash
.venv/Scripts/python -m smartdialer.cli sim --scenario all --out loadtest/results
.venv/Scripts/python -m smartdialer.cli sim --scenario D --seed 42 --duration 600 --agents 100
```

Each scenario runs on a `VirtualClock` against the real allocator, safety
controller, and Postgres — nothing is stubbed. Default scenario durations are
shortened from the assignment's illustrative 30-minute runs so the full suite
finishes in a few minutes of *wall* time (the virtual clock compresses
simulated time, not the database round trips each tick makes); pass
`--duration`/`--agents` for a longer, denser run. Output: one CSV per
scenario plus four charts per scenario and a `summary.md` in
`loadtest/results/`.

## Run the load test

```bash
.venv/Scripts/python loadtest/run_load.py --agents 100 500 1000 2000
```

Measures the two things ARCHITECTURE.md §15.1 names as the first bottlenecks —
the pacing snapshot query and CAS contention on agent reservation — at
increasing campaign size, and directly compares the naive `count(*)/GROUP BY`
snapshot against the trigger-maintained `campaign_counters` O(1) read
(`migrations/002_counters.sql`). Output: `loadtest/results/scale.csv` and
`scale_latency.png`.

## The five things to look at

1. [`repo/agents.py::RESERVE_CAS`](src/smartdialer/repo/agents.py) — how two
   workers cannot reserve one agent.
2. [`domain/transitions.py::can_apply`](src/smartdialer/domain/transitions.py)
   — how out-of-order and duplicate events die.
3. [`safety/controller.py`](src/smartdialer/safety/controller.py) — the
   boundary, and why it cannot be switched off.
4. [`pacing/predictive.py::decide`](src/smartdialer/pacing/predictive.py) —
   the arithmetic, logged verbatim every tick into `pacing_decisions`.
5. [`worker/reaper.py`](src/smartdialer/worker/reaper.py) — what happens when
   a worker dies mid-dial.

## Invariants (see ARCHITECTURE.md §7 for the full table)

| # | Invariant | Enforced by | Proven by |
|---|---|---|---|
| I1 | An agent is reserved by at most one worker at any instant | Conditional `UPDATE` on `(state, version)` | `test_agent_reservation.py`, `property/test_concurrent_reservation.py` |
| I2 | A borrower has at most one non-terminal call | `FOR UPDATE SKIP LOCKED` claim + `UNIQUE(idempotency_key)` | `test_borrower_claim.py` |
| I5 | A provider event is applied at most once | `UNIQUE(provider, provider_event_id)` | `test_event_dedup.py` |
| I6/I7 | Call state rank is monotonically non-decreasing; terminal never reopens | Rank guard in `repo/calls.py::ADVANCE` | `test_out_of_order.py`, `property/test_event_permutations.py` |
| I8 | No agent stays in `RESERVED`/`DIALING` longer than its lease | Reaper at 1 Hz | `test_crash_recovery.py` |
| I9 | The pacing engine cannot place a call | No allocator/provider reference reachable from its types | `test_safety_boundary.py` (AST import-graph assertion) |
| I10 | The safety controller cannot be disabled | No boolean flag exists; thresholds clamped in `SafetyLimits.__post_init__` | `test_safety_controller.py` |

## Failure demos

```bash
.venv/Scripts/python -m smartdialer.cli sim --scenario E_outage      # provider outage
.venv/Scripts/python -m smartdialer.cli sim --scenario F_agentdrop   # agents vanish mid-run
.venv/Scripts/python -m pytest tests/test_crash_recovery.py -v
.venv/Scripts/python -m pytest tests/test_out_of_order.py -v
```

## Watching it live (bonus, not graded — ARCHITECTURE.md is explicit: no UI)

Two ops conveniences built on top of the graded system, both read-only
against the same tables `pacing_decisions` and `psql` would show:

```bash
# terminal, auto-refreshing:
.venv/Scripts/python -m smartdialer.cli watch --campaign-id 1

# browser dashboard — start the API, then open http://localhost:8000/dashboard
.venv/Scripts/python -m smartdialer.cli api
```

The dashboard shows live agent/call/borrower state distributions,
utilization, the pacing-decision log with each tick's full rationale
(expand "rationale" on any row), and a recent-events feed — served as a
static page + two JSON endpoints (`/api/campaigns`, `/api/status/{id}`) by
the same FastAPI process that ingests webhooks. Shared query logic lives in
`live_status.py` so the terminal and browser views can't drift apart.

## Implementation notes: judgment calls beyond the architecture doc

Six decisions in the running code diverge from the architecture doc's
literal pseudocode, all found by actually running the concurrent workload
against real Postgres — including a live worker — rather than reasoning
about it in the abstract:

1. **`allocate_batch` places its calls concurrently (`asyncio.gather`), not
   sequentially.** A sequential loop would make one tick's allocation work
   take up to N x the provider's round-trip latency instead of roughly one
   round trip — serialising on network latency the same CAS/SKIP LOCKED
   design is supposed to make unnecessary. Concurrent placement can trigger a
   genuine Postgres deadlock under FK-check contention even though every
   transaction only ever touches its own agent/borrower rows
   (`DeadlockDetectedError`) — not just in the initial reserve, but in the
   later "record INITIATED" step and in the pullback sweep's per-call cancel
   too, once call volume was high enough (only visible after fixing note 6
   below let agents actually keep cycling instead of stalling after one
   call). All three retry a bounded number of times, the standard mitigation
   for deadlocks under concurrent contention, and otherwise treat it as any
   other lost race. See `allocator/allocator.py`.
2. **The overdial ceiling (S6, and the pullback sweep) is built on
   `agents_staffed` — total logged-in agents — not `agents_available`.**
   `agents_available` shrinks the instant agents legitimately go busy dialing
   calls the system itself just placed. A ceiling built on it alone shrinks
   in lockstep with ordinary utilization, which made the pullback sweep
   cancel calls it had approved moments earlier — self-reinforcing churn, not
   a safety response to an actual capacity drop. `agents_staffed` only
   changes when agents log in/out or wrap up, which is the thing S6 is
   actually meant to react to. See the comment on
   `MetricsSnapshot.agents_staffed` and `safety/rules.py::s6_overdial_cap`.
3. **The mock providers' event-reordering buffer is keyed per call
   (`pcid`), not a single shared slot.** A single "held event" slot shared
   across every concurrently in-flight call on one provider instance let an
   event that genuinely belonged to call A get paired with, and flushed
   under, call B's `pcid` the moment two calls were in flight at once —
   handing call B a stray event (e.g. FAILED) it never actually received,
   independent of and in addition to the deliberate dup/reorder injection.
   Caught by watching a live worker's DB state and finding a call with
   `answered_at`/`connected_at` set that nonetheless ended `FAILED` with no
   `failure_reason` a few milliseconds later. See `providers/mock_a.py::_emit`.
4. **The pullback sweep re-checks call state at cancel time
   (`cancel_if_not_answered`), not just the rank guard.** It selects
   "currently ringing" candidates in one transaction and cancels them in
   later, per-candidate ones; a candidate can legitimately be answered in
   that gap. The generic rank guard alone doesn't stop the cancel —
   CONNECTED's rank is lower than CANCELLED's, so `state_rank < 6` still
   passes — which would hang up on someone who just picked up, the opposite
   of the "zero compliance cost" property this sweep exists for. Caught
   investigating the same live call from note 3: a second call had been
   `CANCELLED` with reason `SAFETY_PULLBACK` despite `answered_at` being set.
   See `repo/calls.py::CANCEL_IF_NOT_ANSWERED`.
5. **`WRAP_UP -> AVAILABLE` ("wrap timer elapsed" in the agent state
   diagram, §4.2) was missing entirely.** Nothing in the original build ever
   fired it, so every agent got permanently stuck the moment it completed its
   first call — invisible in short test runs (calls take a while to reach
   COMPLETED) but unmistakable watching a live worker for a few minutes:
   agent counts never recovered. Fixed by reusing the same lease mechanism
   RESERVED/DIALING already use — `event_applier.py` writes
   `clock.now() + wrapup_s` as the lease when a call COMPLETEs, and the
   reaper releases it the same way it releases any other expired lease. A
   wall-clock "N real seconds since state_changed_at" comparison would have
   been wrong under the simulator's VirtualClock, the same class of mistake
   as the lease-seeding note in ADR-0005. See `worker/reaper.py::RELEASE_WRAPUP`.
6. **The reaper compares every lease against the caller's own
   `clock.now()`, not Postgres' bare `now()`.** This is the deeper bug note
   5's fix exposed: leases are written as `to_timestamp(clock.now() +
   duration)`, and comparing that against Postgres' real `now()` happens to
   work under `RealClock` (clock time == wall time) but not under the
   simulator's `VirtualClock`, which is seeded far ahead of real elapsed
   time and races further ahead as ticks process. A lease computed from an
   already-advanced virtual "now" could require far more *real* wall-clock
   time to satisfy than the simulation's entire run takes — so it would
   simply never expire, and every agent would still get stuck after one
   call, just less obviously than note 5 alone. This affected every
   lease-based reclaim in the reaper (`RESERVED`/`DIALING`/orphan calls too),
   not only `WRAP_UP` — it was just invisible for those because normal event
   flow usually resolves them before their timeout matters. Caught by
   re-running the simulator after fixing note 5 and finding `calls_connected`
   still capped at exactly the agent count. See `worker/reaper.py`'s
   `to_timestamp($1)` comparisons and `test_reaper_clock_consistency.py`.

## Why this stack

Every hard problem in this assignment is either mutual exclusion or
idempotency. PostgreSQL solves both natively and *transactionally*: a
conditional `UPDATE` gives compare-and-swap agent reservation, `SELECT ... FOR
UPDATE SKIP LOCKED` gives lock-free work partitioning across N workers, a
`UNIQUE` constraint gives exactly-once provider-event application, and
`pg_advisory_lock` gives leader election for the pacing loop. Adding Redis
would introduce exactly the cache-vs-database split-brain the assignment asks
about; adding Kafka would give at-least-once delivery I'd still have to
dedupe in Postgres. The queue, the lock manager, and the state store are one
system with one consistency model.

**What it makes harder:** a single write node caps this somewhere around
5-10k agents, and the pacing loop's aggregate queries are the first thing to
degrade — addressed in the section below rather than hand-waved away.

## What breaks at scale (§15 of ARCHITECTURE.md)

1. **~1,000 agents** — the pacing snapshot's `GROUP BY state` scan misses its
   250ms tick deadline. Fix: `campaign_counters`, a single row maintained
   incrementally by a trigger inside the same transaction as each state
   change (`migrations/002_counters.sql`) — snapshot becomes an O(1)
   primary-key read.
2. **~3,000 agents** — the counters row itself becomes hot-row contention.
   Fix: shard it `(campaign_id, shard_id)` across N shards; sum on read.
3. **~10,000 agents** — provider *events*, not calls, are the volume driver
   (~6 events/call). Fix, in order: batch-insert webhooks, partition
   `provider_events` by day, and only then consider a durable queue in front
   of ingest.
4. **~10,000+ agents** — single Postgres write node. Fix: campaigns are a
   natural shard key (they share no agents or borrowers).

What does *not* break: agent reservation contention is proportional to
`workers × tick_rate`, not agent count — `SKIP LOCKED` means workers never
queue behind each other, and a larger candidate pool only *reduces* CAS
collision rate.

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full design (state machines,
pacing derivation, safety rule table, failure scenarios) and
[docs/adr/](docs/adr/) for the six architecture decision records. The answer
to the assignment's closing question is in
[docs/final-answer.md](docs/final-answer.md).
