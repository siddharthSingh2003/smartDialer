# SmartDialer — Architecture & Build Guide

> A single document that contains everything needed to design, build, test, and defend
> the SmartDialer assignment. Read top-to-bottom once, then build phase by phase.
> No external documentation lookup should be required.

---

## Table of contents

| # | Section |
|---|---------|
| 1 | [Scope and grading strategy](#1-scope-and-grading-strategy) |
| 2 | [Technology decisions](#2-technology-decisions) |
| 3 | [System architecture](#3-system-architecture) |
| 4 | [Agent state machine](#4-agent-state-machine) |
| 5 | [Call state machine](#5-call-state-machine) |
| 6 | [Database schema](#6-database-schema) |
| 7 | [Core invariants](#7-core-invariants) |
| 8 | [Complete file tree](#8-complete-file-tree) |
| 9 | [Build phases](#9-build-phases) |
| 10 | [Pacing mathematics](#10-pacing-mathematics) |
| 11 | [Safety controller specification](#11-safety-controller-specification) |
| 12 | [Failure scenarios](#12-failure-scenarios) |
| 13 | [Simulation plan](#13-simulation-plan) |
| 14 | [Testing strategy](#14-testing-strategy) |
| 15 | [Load test and scale analysis](#15-load-test-and-scale-analysis) |
| 16 | [Architecture decision records](#16-architecture-decision-records) |
| 17 | [Interview defence sheet](#17-interview-defence-sheet) |
| 18 | [Final question answer](#18-final-question-answer) |
| 19 | [README content](#19-readme-content) |

---

## 1. Scope and grading strategy

### 1.1 What the grader actually weights

| Area | Weight | Where it is earned in this build |
|---|---|---|
| System design | 20% | §3, §16 ADRs, the fact that the stack is small |
| Distributed systems & concurrency | 15% | §7 invariants, Phase 3 atomic reservation, Phase 9 leases |
| Progressive dialing | 10% | Phase 6 |
| Predictive pacing | 15% | Phase 7 + §10 maths + `pacing_decisions` audit table |
| Safety & correctness | 15% | Phase 8, structural non-bypassability |
| Failure handling | 10% | Phase 9 + §12 demos |
| Testing & performance | 10% | Phase 11, Phase 12 |
| Code quality & documentation | 5% | This file + README |

**65% of the score is architecture and reasoning. 5% is code quality.**
Therefore: build a *small* system with a *bulletproof* concurrency story and a
*self-explaining* pacing decision log. Do not build a dashboard. Do not build
microservices.

### 1.2 Non-goals (state these explicitly in the README)

- No real-time media handling, no SIP, no audio.
- No ML model. A statistical estimator with a confidence bound is deliberate, not a shortcut.
- No horizontal scale test at 10,000 agents. We reason about the bottleneck instead (§15).
- No UI. A CSV + matplotlib report is the deliverable for simulation.

### 1.3 Time budget (6 hours)

| Time | Phase |
|---|---|
| 0:00–0:20 | Phase 0 — bootstrap |
| 0:20–0:50 | Phase 1 — domain + schema |
| 0:50–1:05 | Phase 2 — clock + db layer |
| 1:05–1:45 | Phase 3 — agent allocation (the concurrency core) |
| 1:45–2:20 | Phase 4 — call lifecycle + event ledger |
| 2:20–2:50 | Phase 5 — providers + circuit breaker |
| 2:50–3:15 | Phase 6 — progressive dialer (**first working system**) |
| 3:15–3:50 | Phase 7 — predictive pacing |
| 3:50–4:15 | Phase 8 — safety controller |
| 4:15–4:35 | Phase 9 — reaper / crash recovery |
| 4:35–5:10 | Phase 10 — simulator + scenarios |
| 5:10–5:35 | Phase 11 — tests |
| 5:35–5:50 | Phase 12 — load test |
| 5:50–6:00 | Phase 13 — docs, diagrams, final answer |

**Cut rule:** if you are behind, cut Phase 12 and the Plivo integration. Never cut
Phase 8 or Phase 13. A working progressive dialer with an excellent decision document
beats a half-finished predictive engine with no docs.

---

## 2. Technology decisions

### 2.1 The stack

| Concern | Choice | Alternative rejected |
|---|---|---|
| Language | Python 3.11 + asyncio | Go — equally valid, pick what you are fast in |
| Store | PostgreSQL 15 | Redis, Kafka, Mongo |
| DB driver | `asyncpg` (raw SQL) | SQLAlchemy ORM — hides the locking semantics we are being graded on |
| Webhook ingest | FastAPI + uvicorn | anything |
| Tests | pytest, pytest-asyncio, hypothesis | — |
| Charts | matplotlib | — |
| Orchestration | docker compose (Postgres only) | k8s |

### 2.2 The one-paragraph defence (memorise this)

> Every hard problem in this assignment is either mutual exclusion or idempotency.
> PostgreSQL solves both natively and *transactionally*: a conditional `UPDATE` gives
> me compare-and-swap agent reservation, `SELECT … FOR UPDATE SKIP LOCKED` gives me
> lock-free work partitioning across N workers, a `UNIQUE` constraint gives me exactly-once
> provider event application, and `pg_advisory_lock` gives me leader election for the
> pacing loop. Adding Redis would introduce exactly the cache-vs-database split-brain the
> assignment asks about in the interview. Adding Kafka would give me at-least-once delivery
> that I would still have to deduplicate in Postgres. So the queue, the lock manager, and
> the state store are one system with one consistency model.
> **What it makes harder:** a single write node caps me somewhere around 5–10k agents, and
> the pacing loop's aggregate queries are the first thing to degrade. I address that in the
> scale section rather than pretending it away.

### 2.3 What each choice makes harder (say this before they ask)

| Choice | Cost |
|---|---|
| Single Postgres | Write throughput ceiling; §15 explains where and the fix order |
| Raw SQL | More boilerplate than an ORM; accepted for explicit lock semantics |
| asyncio single-process worker | GIL-bound; but the workload is IO-bound, and we run N processes for parallelism |
| Virtual clock everywhere | Every `sleep` must go through the clock abstraction; enforced by a lint test |
| No message broker | Webhook bursts hit Postgres directly; batched insert mitigates until ~10k agents |

---

## 3. System architecture

### 3.1 Required pipeline

```
Campaign → Pacing Engine (Progressive | Predictive) → Safety Controller → Call Allocator → Telecom Provider
```

The pacing engine **never** holds a reference to the allocator or a provider. It is a
pure function of a metrics snapshot and returns a value object. This is the structural
guarantee that satisfies *"the predictive algorithm should not have a way to simply switch
the safety mechanism off."*

### 3.2 Component diagram

```mermaid
flowchart TB
    subgraph Control["Control plane (one leader per campaign, pg_advisory_lock)"]
        SNAP["MetricsSnapshot\n(read-only aggregate)"]
        PE["Pacing Engine\nprogressive | predictive\nPURE FUNCTION"]
        SC["Safety Controller\nowns allocator handle"]
        SNAP --> PE
        PE -->|PacingRequest\nvalue object| SC
    end

    subgraph Data["PostgreSQL (single source of truth)"]
        AG[(agents)]
        BO[(borrowers)]
        CA[(calls)]
        EV[(provider_events)]
        PD[(pacing_decisions)]
    end

    subgraph Workers["Dialer workers 1..N (stateless)"]
        AL["Call Allocator\nCAS reserve agent\nSKIP LOCKED claim borrower"]
        RE["Lease Reaper\n1 Hz"]
        EA["Event Applier\nrank-guarded transitions"]
    end

    subgraph Prov["Provider layer"]
        REG["Provider Registry\n+ Circuit Breaker"]
        PA["MockProviderA\nfast, reliable"]
        PB["MockProviderB\nslow, dupes, reorder"]
        PL["PlivoProvider\noptional"]
    end

    SC -->|SafetyDecision\napproved_n, reason_code| AL
    SC --> PD
    AL --> AG
    AL --> BO
    AL --> CA
    AL --> REG
    REG --> PA & PB & PL
    PA & PB & PL -->|webhooks| WH["FastAPI /webhooks/:provider"]
    WH -->|INSERT ON CONFLICT DO NOTHING| EV
    EV --> EA
    EA --> CA
    EA --> AG
    RE --> AG & CA
    Data --> SNAP
    REG -.health.-> SNAP
```

### 3.3 Control flow of one pacing tick

```mermaid
sequenceDiagram
    participant L as Pacing Loop (leader)
    participant DB as Postgres
    participant PE as Pacing Engine
    participant SC as Safety Controller
    participant AL as Allocator
    participant P as Provider

    L->>DB: SELECT counters (single aggregate row)
    DB-->>L: MetricsSnapshot
    L->>PE: decide(snapshot)
    PE-->>L: PacingRequest(n=17, inputs)
    L->>SC: evaluate(request, snapshot)
    SC->>DB: INSERT pacing_decisions (requested, approved, reason)
    SC-->>L: SafetyDecision(approved=9, reason=OVERDIAL_CAP)
    loop 9 times
        L->>AL: allocate_one()
        AL->>DB: UPDATE agents SET RESERVED WHERE state=AVAILABLE AND version=$v
        alt 0 rows
            AL-->>L: lost race, skip
        else 1 row
            AL->>DB: claim borrower (FOR UPDATE SKIP LOCKED)
            AL->>DB: INSERT call (idempotency_key unique)
            AL->>P: place_call()
            AL->>DB: UPDATE call → INITIATED, agent → DIALING, set lease
        end
    end
```

### 3.4 Process topology for the demo

| Process | Count | Responsibility |
|---|---|---|
| `smartdialer worker` | 3 | pacing leader election, allocation, event application, reaper |
| `smartdialer api` | 1 | webhook ingest |
| `postgres` | 1 | everything stateful |
| `smartdialer sim` | 1 | drives the virtual clock, injects failures, writes CSV |

All three workers are identical binaries. Exactly one wins `pg_try_advisory_lock(campaign_id)`
and runs the pacing loop; the other two still do allocation, event application, and reaping.
Kill the leader and another takes over within one lock-poll interval (1s).

---

## 4. Agent state machine

### 4.1 States

| State | Meaning | Holds a lease? |
|---|---|---|
| `OFFLINE` | Not logged in | No |
| `AVAILABLE` | Logged in, idle, allocatable | No |
| `RESERVED` | Claimed by a worker, call not yet placed | **Yes** (30s) |
| `DIALING` | Call handed to provider, awaiting answer | **Yes** (setup timeout + margin) |
| `CONNECTED` | Bridged with a borrower | No (call events drive it) |
| `WRAP_UP` | Post-call disposition | No (timer-driven) |
| `PAUSED` | Logged in, self-marked unavailable | No |

### 4.2 Diagram

```mermaid
stateDiagram-v2
    [*] --> OFFLINE
    OFFLINE --> AVAILABLE: login
    AVAILABLE --> RESERVED: CAS reserve (worker wins race)
    AVAILABLE --> PAUSED: agent pauses
    AVAILABLE --> OFFLINE: logout
    RESERVED --> DIALING: call INITIATED
    RESERVED --> AVAILABLE: allocation aborted / lease expired
    RESERVED --> OFFLINE: agent vanished (reaper detects)
    DIALING --> CONNECTED: call ANSWERED+bridged
    DIALING --> AVAILABLE: call FAILED / CANCELLED / no-answer
    DIALING --> OFFLINE: agent vanished (call cancelled by reaper)
    CONNECTED --> WRAP_UP: call COMPLETED
    WRAP_UP --> AVAILABLE: wrap timer elapsed
    WRAP_UP --> PAUSED: agent pauses after wrap
    PAUSED --> AVAILABLE: agent resumes
    PAUSED --> OFFLINE: logout
    CONNECTED --> OFFLINE: agent drops mid-call (rare, reaper)
```

### 4.3 The reservation race — the answer they will ask for

> **"Two workers see the same AVAILABLE agent at almost the same time. Both must not be
> able to reserve it. Explain how you prevent it."**

There is no read-then-write anywhere in the allocation path. Reservation is a single
conditional `UPDATE` executed by the database:

```sql
UPDATE agents
   SET state             = 'RESERVED',
       version           = version + 1,
       lease_owner       = $1,
       lease_expires_at  = $2,
       updated_at        = $2
 WHERE id = $3
   AND state = 'AVAILABLE'
   AND version = $4
RETURNING id, version;
```

Postgres takes a row-level exclusive lock for the duration of the statement. The second
worker's statement blocks until the first commits, then re-evaluates the `WHERE` clause
against the new tuple, finds `state <> 'AVAILABLE'` (and `version` bumped), and affects
**zero rows**. Zero rows returned = "you lost the race, move on to the next candidate."

Two independent guards are stacked deliberately:

1. **`version` (optimistic CAS)** — catches ABA: agent goes AVAILABLE → RESERVED → released
   → AVAILABLE between a worker's read and its write. The state predicate alone would let
   that stale write through; the version predicate will not.
2. **`state = 'AVAILABLE'` predicate** — the primary business guard.

For batch acquisition (predictive mode requests 9 agents at once) use lock-free partitioning
so workers do not serialise on each other:

```sql
SELECT id, version
  FROM agents
 WHERE campaign_id = $1 AND state = 'AVAILABLE'
 ORDER BY updated_at ASC          -- fairest agent first
 LIMIT $2
 FOR UPDATE SKIP LOCKED;
```

`SKIP LOCKED` makes two concurrent workers receive **disjoint** row sets by construction —
no waiting, no double allocation. The same pattern claims borrowers, which is what prevents
two workers from dialing the same phone number.

---

## 5. Call state machine

### 5.1 States and monotonic rank

The rank is the mechanism that makes out-of-order provider events harmless.

| State | Rank | Terminal | Notes |
|---|---|---|---|
| `QUEUED` | 0 | no | Row created, nothing reserved |
| `RESERVED` | 1 | no | Agent + borrower held |
| `INITIATED` | 2 | no | Provider accepted the request |
| `RINGING` | 3 | no | Provider reports ringing |
| `ANSWERED` | 4 | no | Borrower picked up, not yet bridged |
| `CONNECTED` | 5 | no | Bridged to the agent |
| `COMPLETED` | 6 | **yes** | Normal end |
| `FAILED` | 6 | **yes** | Provider error / busy / no-answer |
| `CANCELLED` | 6 | **yes** | We hung up before answer |
| `ABANDONED` | 6 | **yes** | Answered but no agent available — the compliance event |

### 5.2 Diagram

```mermaid
stateDiagram-v2
    [*] --> QUEUED
    QUEUED --> RESERVED: agent+borrower claimed
    RESERVED --> INITIATED: provider.place_call() ok
    RESERVED --> FAILED: provider rejected / no agent
    INITIATED --> RINGING: provider event
    INITIATED --> FAILED: timeout / error
    INITIATED --> CANCELLED: safety pullback
    RINGING --> ANSWERED: provider event
    RINGING --> FAILED: busy / no answer
    RINGING --> CANCELLED: safety pullback (FREE — no compliance cost)
    ANSWERED --> CONNECTED: agent bridge succeeded
    ANSWERED --> ABANDONED: no agent within grace window
    CONNECTED --> COMPLETED: hangup
    ANSWERED --> COMPLETED: hangup before bridge
    COMPLETED --> [*]
    FAILED --> [*]
    CANCELLED --> [*]
    ABANDONED --> [*]
```

### 5.3 Two independent guards against provider misbehaviour

**Guard 1 — deduplication (idempotency).** Every inbound event is inserted into an
append-only ledger *before* anything else happens:

```sql
INSERT INTO provider_events (provider, provider_event_id, call_id, event_type, provider_ts, payload)
VALUES ($1,$2,$3,$4,$5,$6)
ON CONFLICT (provider, provider_event_id) DO NOTHING
RETURNING id;
```

Zero rows returned → this event has already been seen → return HTTP 200 and stop. Providers
retry on non-2xx, so returning 200 for duplicates is required, not optional.
`ANSWERED, ANSWERED, ANSWERED, COMPLETED` therefore produces **one** answered transition and
**one** completed transition.

**Guard 2 — monotonic rank (ordering).** A transition is applied only if it is both
*legal* and *forward*:

```python
def can_apply(current: CallState, incoming: CallState) -> bool:
    if RANK[current] >= TERMINAL_RANK:      # terminal absorbs everything
        return False
    if RANK[incoming] <= RANK[current]:     # stale / out-of-order
        return False
    return incoming in LEGAL_NEXT[current]  # explicit transition table
```

Applied to the assignment's evil sequence `COMPLETED, ANSWERED, RINGING`:

| Event | Current | Rank test | Result |
|---|---|---|---|
| COMPLETED | INITIATED (2) | 6 > 2, legal | applied → COMPLETED |
| ANSWERED | COMPLETED (6) | terminal | logged as `out_of_order`, **not applied** |
| RINGING | COMPLETED (6) | terminal | logged as `out_of_order`, **not applied** |

The late events are never silently dropped — they are stored in the ledger with
`applied = false` and an `anomaly` reason, which is what you show the interviewer.

**Side effects belong to the transition, not the event.** The agent bridge and the counter
increments happen inside the same transaction that advances the call rank. Because that
transaction commits at most once per rank level, the side effect fires at most once even
under duplicate delivery.

### 5.4 Worker crashes immediately after ANSWERED

The event is already durable in `provider_events` (it was inserted before any processing).
On restart, the event applier scans for `applied = false` rows and replays them.
If `COMPLETED` arrives in the meantime, the ledger holds both; replay applies them in rank
order and lands on `COMPLETED`. The agent's `DIALING` lease expires within 30 seconds and
the reaper reconciles it back to `AVAILABLE` even if no event ever arrives.

---

## 6. Database schema

`migrations/001_init.sql` — copy verbatim.

```sql
CREATE TYPE agent_state AS ENUM
    ('OFFLINE','AVAILABLE','RESERVED','DIALING','CONNECTED','WRAP_UP','PAUSED');

CREATE TYPE call_state AS ENUM
    ('QUEUED','RESERVED','INITIATED','RINGING','ANSWERED','CONNECTED',
     'COMPLETED','FAILED','CANCELLED','ABANDONED');

CREATE TYPE borrower_state AS ENUM
    ('PENDING','LOCKED','IN_CALL','DONE','EXHAUSTED','SUPPRESSED');

CREATE TYPE pacing_mode AS ENUM ('PROGRESSIVE','PREDICTIVE');

-- ---------------------------------------------------------------- campaigns
CREATE TABLE campaigns (
    id                  BIGSERIAL PRIMARY KEY,
    name                TEXT        NOT NULL,
    mode                pacing_mode NOT NULL DEFAULT 'PROGRESSIVE',
    is_active           BOOLEAN     NOT NULL DEFAULT true,
    max_overdial_ratio  NUMERIC(4,2) NOT NULL DEFAULT 1.50,  -- hard cap, floor-guarded
    max_abandon_rate    NUMERIC(4,3) NOT NULL DEFAULT 0.030, -- 3% regulatory-style budget
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ------------------------------------------------------------------- agents
CREATE TABLE agents (
    id                BIGSERIAL PRIMARY KEY,
    campaign_id       BIGINT      NOT NULL REFERENCES campaigns(id),
    ext_ref           TEXT        NOT NULL,
    state             agent_state NOT NULL DEFAULT 'OFFLINE',
    version           BIGINT      NOT NULL DEFAULT 0,     -- optimistic CAS token
    lease_owner       TEXT,                               -- worker id holding it
    lease_expires_at  TIMESTAMPTZ,
    current_call_id   BIGINT,
    state_changed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (campaign_id, ext_ref)
);

-- the hot path for allocation: partial index keeps it tiny
CREATE INDEX idx_agents_available
    ON agents (campaign_id, updated_at)
    WHERE state = 'AVAILABLE';

-- the hot path for the reaper
CREATE INDEX idx_agents_leases
    ON agents (lease_expires_at)
    WHERE state IN ('RESERVED','DIALING');

-- ---------------------------------------------------------------- borrowers
CREATE TABLE borrowers (
    id               BIGSERIAL PRIMARY KEY,
    campaign_id      BIGINT         NOT NULL REFERENCES campaigns(id),
    phone            TEXT           NOT NULL,
    priority         INT            NOT NULL DEFAULT 100,  -- lower dials first
    state            borrower_state NOT NULL DEFAULT 'PENDING',
    attempt_count    INT            NOT NULL DEFAULT 0,
    max_attempts     INT            NOT NULL DEFAULT 3,
    next_eligible_at TIMESTAMPTZ    NOT NULL DEFAULT now(),
    locked_by        TEXT,
    locked_until     TIMESTAMPTZ,
    UNIQUE (campaign_id, phone)
);

CREATE INDEX idx_borrowers_dialable
    ON borrowers (campaign_id, priority, next_eligible_at)
    WHERE state = 'PENDING';

-- -------------------------------------------------------------------- calls
CREATE TABLE calls (
    id               BIGSERIAL PRIMARY KEY,
    campaign_id      BIGINT      NOT NULL REFERENCES campaigns(id),
    agent_id         BIGINT      REFERENCES agents(id),
    borrower_id      BIGINT      NOT NULL REFERENCES borrowers(id),
    provider         TEXT        NOT NULL,
    provider_call_id TEXT,
    state            call_state  NOT NULL DEFAULT 'QUEUED',
    state_rank       SMALLINT    NOT NULL DEFAULT 0,   -- denormalised for the guard
    attempt_no       INT         NOT NULL DEFAULT 1,
    idempotency_key  TEXT        NOT NULL,             -- campaign:borrower:attempt
    lease_owner      TEXT,
    lease_expires_at TIMESTAMPTZ,
    initiated_at     TIMESTAMPTZ,
    ringing_at       TIMESTAMPTZ,
    answered_at      TIMESTAMPTZ,
    connected_at     TIMESTAMPTZ,
    terminal_at      TIMESTAMPTZ,
    failure_reason   TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (idempotency_key)                            -- retry cannot double-dial
);

CREATE UNIQUE INDEX idx_calls_provider_id
    ON calls (provider, provider_call_id)
    WHERE provider_call_id IS NOT NULL;

CREATE INDEX idx_calls_inflight
    ON calls (campaign_id, state)
    WHERE state IN ('RESERVED','INITIATED','RINGING','ANSWERED','CONNECTED');

CREATE INDEX idx_calls_leases
    ON calls (lease_expires_at)
    WHERE terminal_at IS NULL;

-- ------------------------------------------------- provider event ledger
CREATE TABLE provider_events (
    id                BIGSERIAL PRIMARY KEY,
    provider          TEXT        NOT NULL,
    provider_event_id TEXT        NOT NULL,
    provider_call_id  TEXT,
    call_id           BIGINT      REFERENCES calls(id),
    event_type        TEXT        NOT NULL,
    provider_ts       TIMESTAMPTZ,
    received_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    applied           BOOLEAN     NOT NULL DEFAULT false,
    anomaly           TEXT,        -- NULL | 'DUPLICATE' | 'OUT_OF_ORDER' | 'UNKNOWN_CALL'
    payload           JSONB       NOT NULL DEFAULT '{}',
    UNIQUE (provider, provider_event_id)                -- exactly-once application
);

CREATE INDEX idx_events_unapplied
    ON provider_events (received_at)
    WHERE applied = false;

-- --------------------------------------------------- pacing audit trail
CREATE TABLE pacing_decisions (
    id                BIGSERIAL PRIMARY KEY,
    campaign_id       BIGINT      NOT NULL REFERENCES campaigns(id),
    tick_at           TIMESTAMPTZ NOT NULL,
    sim_tick          BIGINT,                 -- virtual clock tick, for replay
    mode              pacing_mode NOT NULL,
    requested         INT         NOT NULL,
    approved          INT         NOT NULL,
    reason_code       TEXT        NOT NULL,
    inputs            JSONB       NOT NULL    -- full MetricsSnapshot + estimator internals
);

CREATE INDEX idx_decisions_campaign_time ON pacing_decisions (campaign_id, tick_at DESC);

-- ------------------------------------------- O(1) counters (see §15 scale)
CREATE TABLE campaign_counters (
    campaign_id       BIGINT PRIMARY KEY REFERENCES campaigns(id),
    agents_available  INT NOT NULL DEFAULT 0,
    agents_reserved   INT NOT NULL DEFAULT 0,
    agents_dialing    INT NOT NULL DEFAULT 0,
    agents_connected  INT NOT NULL DEFAULT 0,
    agents_wrapup     INT NOT NULL DEFAULT 0,
    calls_ringing     INT NOT NULL DEFAULT 0,
    calls_connected   INT NOT NULL DEFAULT 0,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

> **Why `campaign_counters` exists.** At 100 agents, `SELECT count(*) … GROUP BY state`
> every 250 ms is fine. At 10,000 agents with 5 workers it is the first thing that melts
> (§15). Maintaining counters incrementally inside the same transaction as the state change
> costs one extra `UPDATE` on a hot row and turns the pacing snapshot into a single-row read.
> Build the naive `count(*)` version in Phase 3 and swap it in Phase 12 — but *design* the
> `MetricsSnapshot` interface so the swap is one file.

---

## 7. Core invariants

These are the sentences the interviewer will try to break. Put them in the README verbatim,
and write one test per invariant (§14).

| # | Invariant | Enforced by |
|---|---|---|
| I1 | An agent is reserved by at most one worker at any instant | Conditional `UPDATE` on `(state, version)` |
| I2 | A borrower has at most one non-terminal call | `FOR UPDATE SKIP LOCKED` claim + `UNIQUE(idempotency_key)` |
| I3 | Agent-bound calls in flight ≤ agents available at decision time × overdial cap | Safety Controller rule S1 |
| I4 | In progressive mode, in-flight agent-bound calls ≤ available agents (ratio 1.0) | Safety Controller rule S1 with cap pinned to 1.0 |
| I5 | A provider event is applied at most once | `UNIQUE(provider, provider_event_id)` |
| I6 | Call state rank is monotonically non-decreasing | Rank guard in the applier |
| 17 | A terminal call never leaves its terminal state | Rank guard: terminal absorbs |
| I8 | No agent stays in `RESERVED`/`DIALING` longer than its lease | Reaper at 1 Hz |
| I9 | The pacing engine cannot place a call | It has no allocator/provider reference; enforced by an import-graph test |
| I10 | The safety controller cannot be disabled | No boolean flag exists; thresholds have hard floors clamped in `__post_init__` |

**Consistency rule to state out loud (they will ask the cache question):**

> The database is the *only* allocation authority. Every other source of state — counters,
> in-memory snapshots, provider health — is **advisory** and feeds only the pacing estimate.
> If the snapshot says 50 agents are free and the truth is 30, the worst outcome is that we
> request too many calls and 20 CAS reservations return zero rows. Being wrong about
> *how many* changes how aggressively we ask; it can never cause a double reservation,
> because the reservation itself is a compare-and-swap in the authority. So when the cache
> and the DB disagree, **the DB wins by construction — the cache is never consulted for a
> decision that mutates allocation.**

---

## 8. Complete file tree

```
smartdialer/
├── README.md                       # setup + how to run each demo (§19)
├── ARCHITECTURE.md                 # this document
├── docker-compose.yml              # postgres only
├── pyproject.toml
├── Makefile                        # make db / run / sim / test / load
├── .env.example
│
├── migrations/
│   └── 001_init.sql                # §6 verbatim
│
├── src/smartdialer/
│   ├── __init__.py
│   ├── config.py                   # Settings dataclass, env parsing, threshold FLOORS
│   ├── clock.py                    # Clock protocol, RealClock, VirtualClock
│   ├── db.py                       # asyncpg pool, transaction helper, advisory locks
│   ├── ids.py                      # worker_id(), idempotency_key()
│   ├── logging.py                  # structured JSON logs w/ correlation ids
│   │
│   ├── domain/
│   │   ├── __init__.py
│   │   ├── enums.py                # AgentState, CallState, BorrowerState, ReasonCode
│   │   ├── transitions.py          # RANK, LEGAL_NEXT, can_apply(), TERMINAL
│   │   └── models.py               # Agent, Call, Borrower, MetricsSnapshot,
│   │                               #   PacingRequest, SafetyDecision (frozen dataclasses)
│   │
│   ├── repo/                       # ONLY place raw SQL lives
│   │   ├── __init__.py
│   │   ├── agents.py               # reserve_cas, pick_candidates, release, set_state
│   │   ├── borrowers.py            # claim_batch (SKIP LOCKED), release, mark_attempt
│   │   ├── calls.py                # create, advance_rank_guarded, cancel, list_inflight
│   │   ├── events.py               # ingest (ON CONFLICT), fetch_unapplied, mark_applied
│   │   ├── decisions.py            # record_decision
│   │   └── metrics.py              # snapshot() — naive count(*) then counter-row version
│   │
│   ├── providers/
│   │   ├── __init__.py
│   │   ├── base.py                 # TelecomProvider Protocol, CallRequest, HealthSnapshot
│   │   ├── mock_a.py               # fast + reliable
│   │   ├── mock_b.py               # slow, timeouts, duplicates, reordering, outage window
│   │   ├── plivo.py                # optional real integration (cherry on cake)
│   │   ├── breaker.py              # CircuitBreaker (closed/open/half-open)
│   │   └── registry.py             # health-aware routing + failover
│   │
│   ├── pacing/
│   │   ├── __init__.py
│   │   ├── base.py                 # PacingEngine Protocol: decide(snapshot)->PacingRequest
│   │   ├── progressive.py          # n = available - inflight_agent_bound
│   │   ├── predictive.py           # §10 maths
│   │   └── estimators.py           # WilsonLowerBound, AsymmetricEWMA, SetupTimeEstimator
│   │
│   ├── safety/
│   │   ├── __init__.py
│   │   ├── rules.py                # S1..S7, each returns (allowed_n, ReasonCode)
│   │   └── controller.py           # SafetyController.evaluate() -> SafetyDecision
│   │
│   ├── allocator/
│   │   ├── __init__.py
│   │   ├── allocator.py            # the ONLY module that can place a call
│   │   └── leases.py               # lease durations, renewal helpers
│   │
│   ├── worker/
│   │   ├── __init__.py
│   │   ├── runner.py               # asyncio.gather of the loops below; graceful shutdown
│   │   ├── pacing_loop.py          # leader-elected, 250 ms tick
│   │   ├── event_applier.py        # drains provider_events where applied=false
│   │   └── reaper.py               # 1 Hz lease expiry + orphan reconciliation
│   │
│   ├── api/
│   │   ├── __init__.py
│   │   ├── app.py                  # FastAPI factory
│   │   └── webhooks.py             # POST /webhooks/{provider}  -> ingest, always 200
│   │
│   ├── sim/
│   │   ├── __init__.py
│   │   ├── world.py                # simulated agents + borrowers on the VirtualClock
│   │   ├── scenarios.py            # A/B/C/D + failure injections
│   │   ├── injectors.py            # provider outage, agent mass-logout, latency spike
│   │   ├── runner.py               # runs one scenario, emits per-tick CSV
│   │   └── report.py               # matplotlib charts + markdown summary
│   │
│   └── cli.py                      # typer: worker | api | sim | seed | migrate
│
├── tests/
│   ├── conftest.py                 # pg testcontainer/fixture, seeded VirtualClock
│   ├── test_transitions.py         # I6, I7 unit-level
│   ├── test_agent_reservation.py   # I1 — 50 concurrent tasks, 1 agent
│   ├── test_borrower_claim.py      # I2
│   ├── test_event_dedup.py         # I5 — ANSWERED x3 + COMPLETED
│   ├── test_out_of_order.py        # COMPLETED, ANSWERED, RINGING
│   ├── test_crash_recovery.py      # kill mid-dial, assert reconciliation
│   ├── test_safety_controller.py   # every reason code fires; cap cannot be raised
│   ├── test_safety_boundary.py     # I9 — import-graph assertion
│   ├── test_pacing_math.py         # known snapshot -> known n
│   ├── test_provider_breaker.py    # open/half-open/close transitions
│   └── property/
│       ├── test_event_permutations.py   # hypothesis: any permutation -> same terminal
│       └── test_concurrent_reservation.py
│
├── loadtest/
│   ├── run_load.py                 # 100 / 1000 agents on virtual clock, latency histogram
│   └── results/                    # committed CSV + PNG so the reviewer sees output
│
└── docs/
    ├── adr/
    │   ├── 0001-postgres-only.md
    │   ├── 0002-safety-boundary-is-structural.md
    │   ├── 0003-lease-based-crash-recovery.md
    │   ├── 0004-monotonic-rank-for-ordering.md
    │   ├── 0005-virtual-clock.md
    │   └── 0006-wilson-lower-bound-pacing.md
    ├── diagrams/                   # exported mermaid PNGs
    └── final-answer.md             # §18
```

---

## 9. Build phases

Each phase states: **goal → files → code → acceptance test**. Do not start a phase
before its acceptance test passes for the previous one.

---

### Phase 0 — Bootstrap (20 min)

**Goal:** `make db && make test` runs green on an empty test suite.

**Files:** `docker-compose.yml`, `pyproject.toml`, `Makefile`, `.env.example`, `src/smartdialer/config.py`

```yaml
# docker-compose.yml
services:
  db:
    image: postgres:15-alpine
    environment:
      POSTGRES_USER: dialer
      POSTGRES_PASSWORD: dialer
      POSTGRES_DB: dialer
    ports: ["5432:5432"]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U dialer"]
      interval: 2s
      retries: 15
```

```toml
# pyproject.toml
[project]
name = "smartdialer"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
  "asyncpg>=0.29", "fastapi>=0.110", "uvicorn>=0.29",
  "typer>=0.12", "pydantic>=2.6", "matplotlib>=3.8", "httpx>=0.27",
]
[project.optional-dependencies]
dev = ["pytest>=8", "pytest-asyncio>=0.23", "hypothesis>=6.100", "ruff>=0.4"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
```

```makefile
# Makefile
db:      ; docker compose up -d && sleep 3 && $(MAKE) migrate
migrate: ; psql $$DATABASE_URL -f migrations/001_init.sql
seed:    ; python -m smartdialer.cli seed --agents 100 --borrowers 5000
run:     ; python -m smartdialer.cli worker --id w1
api:     ; python -m smartdialer.cli api
sim:     ; python -m smartdialer.cli sim --scenario all --out loadtest/results
test:    ; pytest -q
load:    ; python loadtest/run_load.py --agents 1000
```

```python
# src/smartdialer/config.py
from dataclasses import dataclass, field
import os

@dataclass(frozen=True)
class SafetyLimits:
    """Hard floors. There is deliberately no `enabled` flag anywhere in this class."""
    max_overdial_ratio: float = 1.5      # in-flight agent-bound calls per available agent
    abs_max_overdial_ratio: float = 2.0  # ceiling nobody can exceed
    max_abandon_rate: float = 0.03
    min_samples_for_predictive: int = 30
    ringing_hard_cap: int = 500
    cooldown_ticks_after_breach: int = 40   # 40 * 250ms = 10s of forced progressive

    def __post_init__(self) -> None:
        # clamp: config cannot loosen safety beyond the compiled-in ceiling
        object.__setattr__(self, "max_overdial_ratio",
                           min(self.max_overdial_ratio, self.abs_max_overdial_ratio))
        object.__setattr__(self, "max_abandon_rate", min(self.max_abandon_rate, 0.05))

@dataclass(frozen=True)
class Settings:
    database_url: str = field(default_factory=lambda: os.environ.get(
        "DATABASE_URL", "postgresql://dialer:dialer@localhost:5432/dialer"))
    tick_ms: int = 250
    reaper_hz: float = 1.0
    agent_reserve_lease_s: int = 30
    call_setup_lease_s: int = 45
    wrapup_s: int = 8
    safety: SafetyLimits = field(default_factory=SafetyLimits)
```

**Acceptance:** `docker compose up -d` healthy, `psql` connects, `pytest` exits 0.

---

### Phase 1 — Domain and schema (30 min)

**Goal:** the state machines exist as data, not as `if` statements scattered in workers.

**Files:** `migrations/001_init.sql` (§6), `domain/enums.py`, `domain/transitions.py`, `domain/models.py`

```python
# src/smartdialer/domain/enums.py
from enum import StrEnum

class AgentState(StrEnum):
    OFFLINE="OFFLINE"; AVAILABLE="AVAILABLE"; RESERVED="RESERVED"
    DIALING="DIALING"; CONNECTED="CONNECTED"; WRAP_UP="WRAP_UP"; PAUSED="PAUSED"

class CallState(StrEnum):
    QUEUED="QUEUED"; RESERVED="RESERVED"; INITIATED="INITIATED"; RINGING="RINGING"
    ANSWERED="ANSWERED"; CONNECTED="CONNECTED"
    COMPLETED="COMPLETED"; FAILED="FAILED"; CANCELLED="CANCELLED"; ABANDONED="ABANDONED"

class ReasonCode(StrEnum):
    OK                       = "OK"
    NO_AGENTS                = "NO_AGENTS"
    OVERDIAL_CAP             = "OVERDIAL_CAP"
    ABANDON_BUDGET_BREACH    = "ABANDON_BUDGET_BREACH"
    PROVIDER_DEGRADED        = "PROVIDER_DEGRADED"
    INSUFFICIENT_SAMPLES     = "INSUFFICIENT_SAMPLES"
    ESTIMATOR_UNSTABLE       = "ESTIMATOR_UNSTABLE"
    RINGING_HARD_CAP         = "RINGING_HARD_CAP"
    COOLDOWN_FORCED_PROGRESSIVE = "COOLDOWN_FORCED_PROGRESSIVE"
    CAMPAIGN_PAUSED          = "CAMPAIGN_PAUSED"
```

```python
# src/smartdialer/domain/transitions.py
from .enums import CallState as C, AgentState as A

RANK: dict[C, int] = {
    C.QUEUED:0, C.RESERVED:1, C.INITIATED:2, C.RINGING:3,
    C.ANSWERED:4, C.CONNECTED:5,
    C.COMPLETED:6, C.FAILED:6, C.CANCELLED:6, C.ABANDONED:6,
}
TERMINAL_RANK = 6
TERMINAL = {s for s, r in RANK.items() if r == TERMINAL_RANK}

LEGAL_NEXT: dict[C, set[C]] = {
    C.QUEUED:    {C.RESERVED, C.FAILED, C.CANCELLED},
    C.RESERVED:  {C.INITIATED, C.FAILED, C.CANCELLED},
    C.INITIATED: {C.RINGING, C.ANSWERED, C.FAILED, C.CANCELLED},
    C.RINGING:   {C.ANSWERED, C.FAILED, C.CANCELLED},
    C.ANSWERED:  {C.CONNECTED, C.ABANDONED, C.COMPLETED, C.FAILED},
    C.CONNECTED: {C.COMPLETED, C.FAILED},
    **{t: set() for t in TERMINAL},
}

def can_apply(current: C, incoming: C) -> tuple[bool, str | None]:
    """Returns (apply?, anomaly_reason)."""
    if RANK[current] >= TERMINAL_RANK:
        return False, "OUT_OF_ORDER"      # terminal absorbs everything
    if RANK[incoming] <= RANK[current]:
        return False, "OUT_OF_ORDER"      # stale or duplicate-by-content
    if incoming not in LEGAL_NEXT[current]:
        return False, "ILLEGAL_TRANSITION"
    return True, None

# agent side effects are keyed to the CALL transition, never to the raw event
AGENT_ON_CALL_STATE: dict[C, A] = {
    C.INITIATED: A.DIALING,
    C.CONNECTED: A.CONNECTED,
    C.COMPLETED: A.WRAP_UP,
    C.FAILED:    A.AVAILABLE,
    C.CANCELLED: A.AVAILABLE,
    C.ABANDONED: A.AVAILABLE,
}
```

```python
# src/smartdialer/domain/models.py
from dataclasses import dataclass, field
from .enums import ReasonCode

@dataclass(frozen=True)
class MetricsSnapshot:
    campaign_id: int
    ts: float                    # clock.now() — virtual or real
    agents_available: int
    agents_reserved: int
    agents_dialing: int
    agents_connected: int
    agents_wrapup: int
    calls_ringing: int           # INITIATED + RINGING
    calls_connected: int
    answer_rate_lb: float        # Wilson lower bound, conservative
    answer_rate_point: float
    answer_samples: int
    avg_setup_s: float
    avg_talk_s: float
    abandon_rate_5m: float
    provider_health: dict[str, float] = field(default_factory=dict)  # name -> 0..1

    @property
    def agent_bound_inflight(self) -> int:
        return self.agents_reserved + self.agents_dialing

@dataclass(frozen=True)
class PacingRequest:
    """Pure value object. Carries NO capability to act."""
    campaign_id: int
    n: int
    rationale: dict                 # every input + intermediate, logged verbatim

@dataclass(frozen=True)
class SafetyDecision:
    approved: int
    reason: ReasonCode
    requested: int
    evaluated: dict
```

**Acceptance:** `tests/test_transitions.py` proves `can_apply` rejects
`(COMPLETED, ANSWERED)` and accepts `(RINGING, ANSWERED)`.

---

### Phase 2 — Clock and DB layer (15 min)

**Goal:** nothing in the codebase calls `time.time()` or `asyncio.sleep()` directly.
This is what makes a 30-minute campaign run in 2 seconds and makes tests deterministic.
Retrofitting it later costs an hour you do not have.

```python
# src/smartdialer/clock.py
import asyncio, heapq, time
from typing import Protocol

class Clock(Protocol):
    def now(self) -> float: ...
    async def sleep(self, seconds: float) -> None: ...

class RealClock:
    def now(self) -> float: return time.time()
    async def sleep(self, s: float) -> None: await asyncio.sleep(s)

class VirtualClock:
    """Discrete-event clock. Time advances only when every task is waiting."""
    def __init__(self, start: float = 0.0):
        self._now = start
        self._waiters: list[tuple[float, int, asyncio.Future]] = []
        self._seq = 0

    def now(self) -> float: return self._now

    async def sleep(self, seconds: float) -> None:
        fut = asyncio.get_running_loop().create_future()
        self._seq += 1
        heapq.heappush(self._waiters, (self._now + seconds, self._seq, fut))
        await fut

    async def advance_to_next(self) -> bool:
        """Let all currently-runnable tasks finish, then jump to the next deadline."""
        await asyncio.sleep(0)
        if not self._waiters:
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
```

```python
# src/smartdialer/db.py
import asyncpg
from contextlib import asynccontextmanager

class Db:
    def __init__(self, pool: asyncpg.Pool): self.pool = pool

    @classmethod
    async def connect(cls, dsn: str, min_size=2, max_size=10) -> "Db":
        return cls(await asyncpg.create_pool(dsn, min_size=min_size, max_size=max_size))

    @asynccontextmanager
    async def tx(self):
        async with self.pool.acquire() as con:
            async with con.transaction():
                yield con

    async def try_leader(self, con, campaign_id: int) -> bool:
        """Session-scoped advisory lock => exactly one pacing loop per campaign."""
        return await con.fetchval("SELECT pg_try_advisory_lock($1)", campaign_id)
```

**Acceptance:** a test that schedules three `clock.sleep()` calls and asserts they resolve
in deadline order while wall-clock elapsed stays under 50 ms.

---

### Phase 3 — Agent allocation, the concurrency core (40 min)

**Goal:** invariant I1 holds under 50 concurrent tasks fighting over 1 agent.
**This phase is 15% of the grade. Do not rush it.**

```python
# src/smartdialer/repo/agents.py
from ..domain.enums import AgentState

RESERVE_CAS = """
UPDATE agents
   SET state = 'RESERVED', version = version + 1,
       lease_owner = $2, lease_expires_at = to_timestamp($3), updated_at = now()
 WHERE id = $1 AND state = 'AVAILABLE' AND version = $4
RETURNING id, version
"""

PICK_CANDIDATES = """
SELECT id, version FROM agents
 WHERE campaign_id = $1 AND state = 'AVAILABLE'
 ORDER BY updated_at ASC
 LIMIT $2
 FOR UPDATE SKIP LOCKED
"""

SET_STATE = """
UPDATE agents
   SET state = $2, version = version + 1, updated_at = now(),
       state_changed_at = now(),
       lease_owner   = CASE WHEN $2 IN ('RESERVED','DIALING') THEN $3 ELSE NULL END,
       lease_expires_at = CASE WHEN $2 IN ('RESERVED','DIALING')
                               THEN to_timestamp($4) ELSE NULL END,
       current_call_id = $5
 WHERE id = $1 AND version = $6
RETURNING id
"""

class AgentRepo:
    def __init__(self, db): self.db = db

    async def pick_candidates(self, con, campaign_id: int, n: int):
        return await con.fetch(PICK_CANDIDATES, campaign_id, n)

    async def reserve(self, con, agent_id: int, version: int,
                      worker: str, lease_until: float) -> bool:
        row = await con.fetchrow(RESERVE_CAS, agent_id, worker, lease_until, version)
        return row is not None          # None == lost the race, caller moves on

    async def set_state(self, con, agent_id, version, state: AgentState,
                        worker=None, lease_until=None, call_id=None) -> bool:
        row = await con.fetchrow(SET_STATE, agent_id, str(state), worker,
                                 lease_until, call_id, version)
        return row is not None
```

**Why `PICK_CANDIDATES` and `RESERVE_CAS` are separate:** the `SKIP LOCKED` select gives
disjoint candidate sets cheaply, but candidates can still go stale between the select and
the reserve (agent logs out, another worker in a different transaction takes it). The CAS
is the authoritative guard; the select is an optimisation. Never rely on the select alone.

**Acceptance test** (`tests/test_agent_reservation.py`):

```python
async def test_only_one_worker_reserves(db, agent_id):
    async def attempt(worker):
        async with db.tx() as con:
            row = await con.fetchrow(
                "SELECT id, version FROM agents WHERE id=$1", agent_id)
            return await AgentRepo(db).reserve(con, row["id"], row["version"],
                                               worker, lease_until=9e9)
    results = await asyncio.gather(*[attempt(f"w{i}") for i in range(50)])
    assert sum(results) == 1          # I1
```

---

### Phase 4 — Call lifecycle and the event ledger (35 min)

**Goal:** duplicates and reordering become non-events. Invariants I5, I6, I7.

```python
# src/smartdialer/repo/events.py
INGEST = """
INSERT INTO provider_events
   (provider, provider_event_id, provider_call_id, event_type, provider_ts, payload)
VALUES ($1,$2,$3,$4,to_timestamp($5),$6)
ON CONFLICT (provider, provider_event_id) DO NOTHING
RETURNING id
"""

class EventRepo:
    async def ingest(self, con, provider, event_id, call_ref, etype, ts, payload) -> int | None:
        """Returns row id, or None if this exact event was already recorded."""
        return await con.fetchval(INGEST, provider, event_id, call_ref, etype, ts, payload)

    async def fetch_unapplied(self, con, limit=200):
        return await con.fetch("""
            SELECT * FROM provider_events
             WHERE applied = false
             ORDER BY received_at
             LIMIT $1 FOR UPDATE SKIP LOCKED""", limit)
```

```python
# src/smartdialer/repo/calls.py
ADVANCE = """
UPDATE calls
   SET state = $2, state_rank = $3,
       provider_call_id = COALESCE(provider_call_id, $4),
       initiated_at = COALESCE(initiated_at, CASE WHEN $2='INITIATED' THEN now() END),
       ringing_at   = COALESCE(ringing_at,   CASE WHEN $2='RINGING'   THEN now() END),
       answered_at  = COALESCE(answered_at,  CASE WHEN $2='ANSWERED'  THEN now() END),
       connected_at = COALESCE(connected_at, CASE WHEN $2='CONNECTED' THEN now() END),
       terminal_at  = CASE WHEN $3 >= 6 THEN now() ELSE terminal_at END,
       lease_expires_at = CASE WHEN $3 >= 6 THEN NULL ELSE to_timestamp($5) END
 WHERE id = $1
   AND state_rank < $3              -- THE ORDERING GUARD, enforced by the database
RETURNING id, agent_id
"""
```

> Note the guard lives in the `WHERE` clause, not in Python. Two workers applying two
> different events for the same call concurrently cannot interleave into an inconsistent
> state: the row lock serialises them and the loser's `state_rank < $3` predicate fails.

```python
# src/smartdialer/worker/event_applier.py
from ..domain.transitions import can_apply, RANK, AGENT_ON_CALL_STATE
from ..domain.enums import CallState

async def apply_one(con, repos, ev) -> None:
    call = await repos.calls.get_by_provider_ref(con, ev["provider"], ev["provider_call_id"])
    if call is None:
        await repos.events.mark(con, ev["id"], applied=False, anomaly="UNKNOWN_CALL")
        return                                   # webhook raced call creation; reaper retries

    incoming = CallState(map_provider_event(ev["event_type"]))
    ok, anomaly = can_apply(CallState(call["state"]), incoming)
    if not ok:
        await repos.events.mark(con, ev["id"], applied=True, anomaly=anomaly)
        return                                   # recorded, deliberately not applied

    row = await repos.calls.advance(con, call["id"], incoming, RANK[incoming],
                                    ev["provider_call_id"])
    if row is None:                              # lost to a concurrent applier
        await repos.events.mark(con, ev["id"], applied=True, anomaly="OUT_OF_ORDER")
        return

    # side effect belongs to the transition, inside the SAME transaction
    if (agent_state := AGENT_ON_CALL_STATE.get(incoming)) and row["agent_id"]:
        await repos.agents.force_state(con, row["agent_id"], agent_state)
    await repos.events.mark(con, ev["id"], applied=True)
```

**Acceptance:**
- `ANSWERED, ANSWERED, ANSWERED, COMPLETED` → exactly 2 applied rows, final state `COMPLETED`,
  agent bridged exactly once.
- `COMPLETED, ANSWERED, RINGING` → final state `COMPLETED`, two rows with
  `anomaly='OUT_OF_ORDER'`, `applied=true`.

---

### Phase 5 — Providers and circuit breaker (30 min)

```python
# src/smartdialer/providers/base.py
from dataclasses import dataclass
from typing import Protocol

@dataclass(frozen=True)
class CallRequest:
    call_id: int; to_number: str; from_number: str; idempotency_key: str

@dataclass(frozen=True)
class HealthSnapshot:
    name: str; healthy: bool; error_rate: float; p95_setup_s: float; open_circuit: bool
    @property
    def score(self) -> float:
        return 0.0 if self.open_circuit else max(0.0, 1.0 - self.error_rate)

class ProviderError(Exception): ...
class ProviderTimeout(ProviderError): ...

class TelecomProvider(Protocol):
    name: str
    async def place_call(self, req: CallRequest) -> str: ...   # -> provider_call_id
    async def cancel(self, provider_call_id: str) -> None: ...
    def health(self) -> HealthSnapshot: ...
```

The dialer only ever sees this Protocol. Provider-specific event names are normalised at the
webhook boundary by a per-provider `map_provider_event()`; nothing downstream knows whether
the call came from Mock A, Mock B, or Plivo.

**Mock behaviour matrix — make them genuinely different:**

| Behaviour | Provider A | Provider B |
|---|---|---|
| Setup latency | 200–400 ms | 800–2500 ms |
| Hard failure rate | 2% | 15% |
| Timeout rate | 0% | 8% |
| Duplicate events | never | 20% of events sent twice |
| Out-of-order | never | 25% chance to swap adjacent events |
| Outage window | none | injectable: 100% timeouts for T seconds |
| Answer distribution | scenario-driven | scenario-driven |

```python
# src/smartdialer/providers/mock_b.py  (sketch — A is the same class with tame params)
class MockProviderB:
    name = "mock_b"
    def __init__(self, clock, rng, sink, cfg): ...

    async def place_call(self, req):
        await self.clock.sleep(self.rng.uniform(0.8, 2.5))
        if self.outage_until > self.clock.now():
            raise ProviderTimeout(self.name)
        if self.rng.random() < 0.08: raise ProviderTimeout(self.name)
        if self.rng.random() < 0.15: raise ProviderError("congestion")
        pcid = f"B-{uuid4()}"
        self._schedule_lifecycle(req, pcid)   # emits RINGING/ANSWERED/COMPLETED webhooks
        return pcid

    def _emit(self, pcid, etype):
        ev = {"provider_event_id": str(uuid4()), "provider_call_id": pcid,
              "event_type": etype, "ts": self.clock.now()}
        self.buffer.append(ev)
        if self.rng.random() < 0.20:          # duplicate delivery
            self.buffer.append(dict(ev))
        if len(self.buffer) >= 2 and self.rng.random() < 0.25:
            self.buffer[-1], self.buffer[-2] = self.buffer[-2], self.buffer[-1]  # reorder
```

```python
# src/smartdialer/providers/breaker.py
class CircuitBreaker:
    """closed -> open on error-rate breach -> half-open probe -> closed."""
    def __init__(self, clock, window_s=10.0, threshold=0.25, min_calls=10, cooldown_s=15.0):
        ...
    def record(self, ok: bool) -> None: ...
    def allow(self) -> bool:
        if self.state == "open":
            if self.clock.now() >= self.opened_at + self.cooldown_s:
                self.state = "half_open"; self.probes = 0
                return True                     # let exactly N probes through
            return False
        return True
```

**Registry routing rule:** pick the healthy provider with the highest `score`; on `open`
circuit, that provider is removed from rotation and its `score` of 0 is fed into
`MetricsSnapshot.provider_health`, which the Safety Controller reads (rule S4).

**Acceptance:** `test_provider_breaker.py` drives 20 failures, asserts `allow()` is False,
advances the virtual clock past the cooldown, asserts half-open, feeds a success, asserts closed.

---

### Phase 6 — Progressive dialer: the first working system (25 min)

**Goal:** end-to-end calls flowing. Commit here. Everything after this is upside.

```python
# src/smartdialer/pacing/progressive.py
class ProgressivePacing:
    mode = "PROGRESSIVE"
    def decide(self, s: MetricsSnapshot) -> PacingRequest:
        n = max(0, s.agents_available - s.agent_bound_inflight)
        return PacingRequest(s.campaign_id, n, {
            "rule": "1:1", "available": s.agents_available,
            "agent_bound_inflight": s.agent_bound_inflight,
        })
```

That is the whole engine. The assignment's requirement — *"if there are 50 available agents,
the system should not create more than 50 agent-bound outbound calls"* — is satisfied by
subtracting calls that already hold an agent, not merely by capping at 50.

```python
# src/smartdialer/allocator/allocator.py  — the ONLY module allowed to place calls
class CallAllocator:
    def __init__(self, db, repos, registry, clock, cfg, worker_id): ...

    async def allocate_batch(self, campaign_id: int, n: int) -> int:
        placed = 0
        for _ in range(n):
            if await self._allocate_one(campaign_id):
                placed += 1
        return placed

    async def _allocate_one(self, campaign_id: int) -> bool:
        # 1. reserve agent (CAS) + claim borrower (SKIP LOCKED) + create call row
        async with self.db.tx() as con:
            cands = await self.repos.agents.pick_candidates(con, campaign_id, 1)
            if not cands: return False
            a = cands[0]
            lease = self.clock.now() + self.cfg.agent_reserve_lease_s
            if not await self.repos.agents.reserve(con, a["id"], a["version"],
                                                   self.worker_id, lease):
                return False                                   # lost the race
            b = await self.repos.borrowers.claim_one(con, campaign_id,
                                                     self.worker_id, lease)
            if b is None:
                await self.repos.agents.release(con, a["id"]); return False
            provider = self.registry.pick()
            if provider is None:
                await self.repos.agents.release(con, a["id"])
                await self.repos.borrowers.release(con, b["id"]); return False
            call_id = await self.repos.calls.create(
                con, campaign_id, a["id"], b["id"], provider.name,
                idem=f"{campaign_id}:{b['id']}:{b['attempt_count']+1}")

        # 2. provider call OUTSIDE the transaction — never hold a row lock across IO
        try:
            pcid = await provider.place_call(CallRequest(call_id, b["phone"],
                                                         self.cfg.caller_id, idem))
            self.registry.record(provider.name, ok=True)
        except ProviderError as e:
            self.registry.record(provider.name, ok=False)
            async with self.db.tx() as con:
                await self.repos.calls.fail(con, call_id, reason=str(e))
                await self.repos.agents.release(con, a["id"])
                await self.repos.borrowers.reschedule(con, b["id"], backoff_s=60)
            return False

        # 3. record INITIATED + agent DIALING
        async with self.db.tx() as con:
            await self.repos.calls.advance(con, call_id, CallState.INITIATED, 2, pcid)
            await self.repos.agents.force_state(con, a["id"], AgentState.DIALING,
                lease_until=self.clock.now() + self.cfg.call_setup_lease_s)
        return True
```

**Two deliberate design points to call out in the interview:**

1. **The provider call happens outside the transaction.** Holding a Postgres row lock across
   a 2-second network call would serialise the whole campaign behind the slowest provider.
   The cost is a window where the DB says `RESERVED` but no provider call exists — which the
   lease + reaper closes.
2. **The row is created before the provider call, not after.** If we crash between the two,
   we have a `RESERVED` call with no `provider_call_id` and an expiring lease; the reaper
   fails it and releases the agent. If we created the row *after*, a crash would leak a live
   provider call with no database record — an untrackable call, which for a collections
   dialer is a compliance incident.

**Acceptance:** seed 10 agents / 100 borrowers, run one worker + Mock A, observe calls reach
`COMPLETED` and agents cycle `AVAILABLE → RESERVED → DIALING → CONNECTED → WRAP_UP → AVAILABLE`.

---

### Phase 7 — Predictive pacing (35 min)

**Goal:** a number you can justify to three decimal places. See §10 for the derivation.

```python
# src/smartdialer/pacing/estimators.py
import math
from collections import deque

class WilsonLowerBound:
    """Conservative answer-rate estimate. Pessimistic when data is thin — by design."""
    def __init__(self, z: float = 1.2816, window: int = 300):   # z for 90% one-sided
        self.z = z
        self.obs: deque[int] = deque(maxlen=window)

    def record(self, answered: bool) -> None: self.obs.append(1 if answered else 0)

    @property
    def n(self) -> int: return len(self.obs)

    @property
    def point(self) -> float: return (sum(self.obs) / self.n) if self.n else 0.0

    def lower_bound(self) -> float:
        n = self.n
        if n == 0: return 0.05                      # near-zero => paces like progressive
        p, z = self.point, self.z
        denom  = 1 + z*z/n
        centre = p + z*z/(2*n)
        margin = z * math.sqrt(p*(1-p)/n + z*z/(4*n*n))
        return max(0.01, (centre - margin) / denom)

class AsymmetricEWMA:
    """Falls fast, rises slow. Protects against a 70%->10% answer-rate collapse."""
    def __init__(self, fast=0.30, slow=0.05):
        self.fast_a, self.slow_a = fast, slow
        self.fast = self.slow = None
    def record(self, x: float) -> None:
        self.fast = x if self.fast is None else self.fast_a*x + (1-self.fast_a)*self.fast
        self.slow = x if self.slow is None else self.slow_a*x + (1-self.slow_a)*self.slow
    @property
    def value(self) -> float:
        if self.fast is None: return 0.0
        return min(self.fast, self.slow)            # asymmetry lives in this min()
```

```python
# src/smartdialer/pacing/predictive.py
import math

class PredictivePacing:
    mode = "PREDICTIVE"
    def __init__(self, cfg): self.cfg = cfg

    def decide(self, s: MetricsSnapshot) -> PacingRequest:
        p = max(0.01, min(1.0, s.answer_rate_lb))

        # agents expected to free during the call-setup window (Little's Law)
        freeing = 0.0
        if s.avg_talk_s > 0:
            freeing = s.calls_connected * (s.avg_setup_s / s.avg_talk_s)

        expected_answers = s.calls_ringing * p
        capacity = s.agents_available + freeing - expected_answers
        n = max(0, math.floor(capacity / p))

        return PacingRequest(s.campaign_id, n, {
            "p_answer_lb": round(p, 4),
            "p_answer_point": round(s.answer_rate_point, 4),
            "samples": s.answer_samples,
            "available": s.agents_available,
            "connected": s.calls_connected,
            "ringing": s.calls_ringing,
            "avg_setup_s": round(s.avg_setup_s, 3),
            "avg_talk_s": round(s.avg_talk_s, 2),
            "freeing_soon": round(freeing, 3),
            "expected_answers_from_ringing": round(expected_answers, 3),
            "net_capacity": round(capacity, 3),
            "formula": "floor((available + freeing_soon - ringing*p) / p)",
        })
```

**The `rationale` dict is the single most valuable artifact in the submission.** It is written
verbatim into `pacing_decisions.inputs`. When they ask *"why did your algorithm decide to
initiate 17 calls instead of 10?"*, you run:

```sql
SELECT tick_at, requested, approved, reason_code, jsonb_pretty(inputs)
  FROM pacing_decisions WHERE campaign_id = 1 ORDER BY id DESC LIMIT 1;
```

and read the arithmetic off the screen.

**Acceptance:** `test_pacing_math.py` — a hand-computed snapshot
(available=10, connected=40, ringing=6, p_lb=0.25, setup=3s, talk=120s) must yield
`freeing = 40*(3/120) = 1.0`, `expected = 1.5`, `capacity = 9.5`, `n = floor(38.0) = 38`
— which the Safety Controller will then cut down hard. That contrast is the point.

---

### Phase 8 — Safety Controller (25 min)

**Goal:** a boundary that cannot be bypassed by construction, not by convention.

**Three structural properties, in the ADR and in the README:**

1. `PacingEngine.decide()` takes a `MetricsSnapshot` and returns a `PacingRequest`.
   Neither type carries a database handle, a provider, or an allocator. The engine
   **physically cannot dial**.
2. `SafetyController` is the only object constructed with the `CallAllocator`.
   `pacing_loop.py` calls `engine.decide()` then `controller.evaluate()`; there is no code
   path from a request to a call that does not pass through `evaluate()`.
3. There is no `enabled` flag, no `bypass`, no `DEBUG_SKIP_SAFETY`. Limits live in a frozen
   dataclass whose `__post_init__` clamps them to compiled-in ceilings (Phase 0), so even a
   malicious config file cannot raise the overdial cap above 2.0 or the abandon budget above 5%.

```python
# src/smartdialer/safety/rules.py
def s0_campaign_active(req, s, ctx):
    return (0, ReasonCode.CAMPAIGN_PAUSED) if not ctx.campaign_active else None

def s1_no_agents(req, s, ctx):
    return (0, ReasonCode.NO_AGENTS) if s.agents_available <= 0 else None

def s2_cooldown(req, s, ctx):
    if ctx.cooldown_ticks_left > 0:
        cap = max(0, s.agents_available - s.agent_bound_inflight)   # progressive fallback
        return (min(req.n, cap), ReasonCode.COOLDOWN_FORCED_PROGRESSIVE)
    return None

def s3_insufficient_samples(req, s, ctx):
    if s.answer_samples < ctx.limits.min_samples_for_predictive:
        cap = max(0, s.agents_available - s.agent_bound_inflight)
        return (min(req.n, cap), ReasonCode.INSUFFICIENT_SAMPLES)
    return None

def s4_provider_health(req, s, ctx):
    best = max(s.provider_health.values(), default=0.0)
    if best <= 0.0:  return (0, ReasonCode.PROVIDER_DEGRADED)
    if best < 0.6:   return (min(req.n, int(req.n * best)), ReasonCode.PROVIDER_DEGRADED)
    return None

def s5_abandon_budget(req, s, ctx):
    if s.abandon_rate_5m > ctx.limits.max_abandon_rate:
        ctx.trip_cooldown()                       # forces progressive for N ticks
        cap = max(0, s.agents_available - s.agent_bound_inflight)
        return (min(req.n, cap), ReasonCode.ABANDON_BUDGET_BREACH)
    return None

def s6_overdial_cap(req, s, ctx):
    ceiling = int(s.agents_available * ctx.limits.max_overdial_ratio)
    allowed = max(0, ceiling - s.agent_bound_inflight - s.calls_ringing)
    if req.n > allowed:
        return (allowed, ReasonCode.OVERDIAL_CAP)
    return None

def s7_ringing_hard_cap(req, s, ctx):
    allowed = max(0, ctx.limits.ringing_hard_cap - s.calls_ringing)
    if req.n > allowed:
        return (allowed, ReasonCode.RINGING_HARD_CAP)
    return None

RULES = [s0_campaign_active, s1_no_agents, s2_cooldown, s3_insufficient_samples,
         s4_provider_health, s5_abandon_budget, s6_overdial_cap, s7_ringing_hard_cap]
```

```python
# src/smartdialer/safety/controller.py
class SafetyController:
    def __init__(self, limits, allocator, decisions_repo, clock):
        self._allocator = allocator          # private: engines never receive this
        ...

    async def evaluate_and_execute(self, req: PacingRequest, s: MetricsSnapshot) -> SafetyDecision:
        approved, reason = req.n, ReasonCode.OK
        applied = []
        for rule in RULES:                    # every rule runs; the MINIMUM wins
            out = rule(req, s, self.ctx)
            if out is None: continue
            n, code = out
            applied.append({"rule": rule.__name__, "cap": n, "reason": str(code)})
            if n < approved:
                approved, reason = n, code
        approved = max(0, min(approved, req.n))

        await self.decisions.record(req.campaign_id, self.clock.now(), req.n,
                                    approved, reason, {**req.rationale,
                                                       "rules_applied": applied,
                                                       "snapshot": asdict(s)})
        if approved > 0:
            await self._allocator.allocate_batch(req.campaign_id, approved)
        return SafetyDecision(approved, reason, req.n, {"rules_applied": applied})
```

Every rule always runs and the **minimum** wins, so the audit row shows every constraint that
was binding, not just the first one hit. That is what you show when they ask which limit was
actually responsible.

**Acceptance (`test_safety_controller.py`):**
- `available=0` → approved 0, reason `NO_AGENTS`, regardless of request size.
- `abandon_rate=0.09` → approved ≤ progressive cap, reason `ABANDON_BUDGET_BREACH`,
  and the next 40 ticks stay progressive.
- Config with `max_overdial_ratio=99` is clamped to 2.0.
- `test_safety_boundary.py`: parse `pacing/*.py` with `ast` and assert no import of
  `allocator`, `providers`, or `repo`. **This test is the proof of I9.**

---

### Phase 9 — Reaper and crash recovery (20 min)

```python
# src/smartdialer/worker/reaper.py
RECLAIM_AGENTS = """
UPDATE agents
   SET state = CASE WHEN state = 'RESERVED' THEN 'AVAILABLE' ELSE state END,
       version = version + 1, lease_owner = NULL, lease_expires_at = NULL,
       updated_at = now()
 WHERE state IN ('RESERVED','DIALING') AND lease_expires_at < now()
RETURNING id, state, current_call_id
"""

FAIL_ORPHAN_CALLS = """
UPDATE calls
   SET state = 'FAILED', state_rank = 6, terminal_at = now(),
       failure_reason = 'LEASE_EXPIRED_NO_EVENTS'
 WHERE terminal_at IS NULL
   AND lease_expires_at < now()
   AND state IN ('QUEUED','RESERVED','INITIATED')
RETURNING id, agent_id, borrower_id, provider, provider_call_id
"""

async def reap(db, repos, registry):
    async with db.tx() as con:
        orphans = await con.fetch(FAIL_ORPHAN_CALLS)
        for o in orphans:
            if o["provider_call_id"]:
                registry.get(o["provider"]).schedule_cancel(o["provider_call_id"])
            await repos.agents.release(con, o["agent_id"])
            await repos.borrowers.reschedule(con, o["borrower_id"], backoff_s=120)
        await con.fetch(RECLAIM_AGENTS)
        await repos.events.retry_unknown_call_events(con)   # webhook-raced-creation case
```

**Ordering matters:** fail the calls first, then reclaim agents. Reversed, you could return an
agent to `AVAILABLE` while its call is still live, and the next tick would reserve it into a
second simultaneous call.

**Deliberate asymmetry:** a `DIALING` agent whose lease expired is *not* auto-returned to
`AVAILABLE` by `RECLAIM_AGENTS` — only `RESERVED` is. `DIALING` means a provider call may be
live; it is released only by the call's own terminal transition (from the orphan sweep above
or from a late webhook). This prevents the worst failure in the system: an agent double-booked
onto a live connected call.

**Acceptance (`test_crash_recovery.py`):** run the full sequence
*agent reserved → borrower reserved → call initiated → worker killed*, advance the virtual clock
past the lease, run the reaper, and assert: call `FAILED`, agent `AVAILABLE`, borrower
`PENDING` with `attempt_count = 1` and a future `next_eligible_at`, and no duplicate call row
when the worker restarts (blocked by `UNIQUE(idempotency_key)`).

---

### Phase 10 — Simulator (35 min)

**Goal:** the four scenarios from the assignment plus injected failures, with charts.

```python
# src/smartdialer/sim/scenarios.py
@dataclass
class Scenario:
    name: str
    answer_rate: float | Callable[[float], float]
    talk_time_s: float | Callable[[float], float]
    agents: int = 100
    duration_s: float = 1800          # 30 virtual minutes
    mode: str = "PREDICTIVE"
    injections: list = field(default_factory=list)

SCENARIOS = {
 "A": Scenario("A", 0.20, 120),
 "B": Scenario("B", 0.50,  90),
 "C": Scenario("C", 0.70, 180),
 "D": Scenario("D",
        answer_rate=lambda t: 0.70 if t < 600 else (0.10 if t < 1200 else 0.45),
        talk_time_s=lambda t: 120 + 60*math.sin(t/300)),
 "E_outage":  Scenario("E", 0.50, 90, injections=[ProviderOutage(at=600, dur=120)]),
 "F_agentdrop": Scenario("F", 0.50, 90, injections=[AgentMassLogout(at=600, count=40)]),
 "G_progressive_baseline": Scenario("G", 0.50, 90, mode="PROGRESSIVE"),
}
```

**Per-tick CSV columns** (`loadtest/results/<scenario>.csv`):

```
tick, sim_time, agents_available, agents_reserved, agents_dialing, agents_connected,
agents_wrapup, utilization, calls_initiated_cum, calls_connected_cum, calls_abandoned_cum,
ringing, p_answer_point, p_answer_lb, samples, requested, approved, reason_code,
abandon_rate_5m, provider_a_health, provider_b_health
```

**Four charts per scenario** (`report.py`):

1. Agent utilization over time — predictive vs the progressive baseline (G) overlaid.
   *This is the headline number: the whole point of predictive dialing.*
2. `requested` vs `approved` — the visual proof the safety controller is doing work.
3. Stacked reason codes over time — which constraint bound, when.
4. Abandonment rate with the 3% budget line drawn.

**Summary table to paste into the README:**

| Scenario | Answer rate | Talk | Utilization | Calls connected | Abandon % | Dominant reason code |
|---|---|---|---|---|---|---|
| A | 20% | 120s | | | | |
| B | 50% | 90s | | | | |
| C | 70% | 180s | | | | |
| D | changing | changing | | | | `ABANDON_BUDGET_BREACH` around t=600 |
| E | 50% + outage | 90s | | | | `PROVIDER_DEGRADED` |
| F | 50% + agent drop | 90s | | | | `OVERDIAL_CAP` |
| G | progressive baseline | 90s | | | 0.0 | `OK` |

Fill it from the actual run. **The D and F rows are the interesting ones** — they show the
system reacting, not just running.

---

### Phase 11 — Tests (25 min)

Property tests earn more credit here than line coverage.

```python
# tests/property/test_event_permutations.py
from hypothesis import given, strategies as st
import itertools

EVENTS = ["RINGING", "ANSWERED", "COMPLETED"]

@given(st.permutations(EVENTS), st.integers(0, 3))
async def test_any_order_reaches_same_terminal(order, extra_dupes):
    """Whatever the order, whatever the duplication, the call ends COMPLETED exactly once."""
    call = await make_call(state="INITIATED")
    stream = list(order)
    for _ in range(extra_dupes):
        stream.insert(rng.randrange(len(stream)+1), rng.choice(stream))
    for e in stream:
        await ingest_and_apply(call, e)
    row = await fetch(call)
    assert row["state"] == "COMPLETED"
    assert row["state_rank"] == 6
    assert await count_agent_bridge_side_effects(call) <= 1
```

```python
# tests/property/test_concurrent_reservation.py
@given(st.integers(1, 20), st.integers(1, 60))
async def test_never_double_reserve(n_agents, n_workers):
    ids = await seed_agents(n_agents, state="AVAILABLE")
    results = await asyncio.gather(*[try_reserve_any() for _ in range(n_workers)])
    reserved = [r for r in results if r]
    assert len(reserved) == len(set(reserved))     # no agent reserved twice
    assert len(reserved) <= n_agents               # I1
```

**Required test list (one per invariant):**

| File | Proves |
|---|---|
| `test_agent_reservation.py` | I1 |
| `test_borrower_claim.py` | I2 |
| `test_safety_controller.py` | I3, I4, I10 |
| `test_event_dedup.py` | I5 |
| `test_out_of_order.py` | I6, I7 |
| `test_crash_recovery.py` | I8 |
| `test_safety_boundary.py` | I9 (AST import-graph assertion) |

---

### Phase 12 — Load test (15 min)

Not "10,000 calls per second". The deliverable is **where it degrades and why**.

```python
# loadtest/run_load.py
# For agents in [100, 500, 1000, 2000]:
#   - seed agents + 50x borrowers
#   - run 5 virtual minutes on the VirtualClock with 3 worker tasks
#   - measure: snapshot query p50/p95/p99, allocation p95, ticks completed,
#              CAS contention rate (zero-row updates / attempts), utilization achieved
# Emit loadtest/results/scale.csv and a latency-vs-agents PNG.
```

Report table (fill from your run — the *shape* is what matters):

| Agents | Snapshot p95 | Alloc p95 | CAS loss rate | Ticks hit target 250 ms? | Utilization |
|---|---|---|---|---|---|
| 100 | | | | yes | |
| 500 | | | | yes | |
| 1000 | | | | marginal | |
| 2000 | | | | no — snapshot dominates | |

Then swap `repo/metrics.py` from `count(*)` to the `campaign_counters` single-row read and
re-run the 2000 case to show the fix working. **Demonstrating a measured bottleneck and its
repair is worth more than a bigger number.**

---

### Phase 13 — Documentation (10 min)

- Export the three mermaid diagrams to `docs/diagrams/*.png`.
- Write the six ADRs (§16) — five sentences each: context, decision, consequence, what it
  makes harder, what would change my mind.
- Write `docs/final-answer.md` (§18).
- README (§19) with copy-pasteable commands for each demo.

---

## 10. Pacing mathematics

### 10.1 The question the formula answers

> Given what I can observe right now, what is the largest number of new calls I can start
> such that the expected number of borrowers who answer does not exceed the number of agents
> who will be free when those answers arrive?

### 10.2 Derivation

Let

| Symbol | Meaning | Source |
|---|---|---|
| `A` | agents currently `AVAILABLE` | counters |
| `R` | calls currently `INITIATED` or `RINGING` | counters |
| `C` | calls currently `CONNECTED` | counters |
| `p` | probability a new call is answered — **lower bound**, not point estimate | Wilson LB |
| `Ts` | mean call setup time (initiate → answer) | EWMA |
| `Tt` | mean talk time | EWMA |
| `n` | calls to start this tick | output |

**Step 1 — agents that will free up during the setup window.**
By Little's Law, a stable system with `C` concurrent calls each lasting `Tt` completes calls
at rate `C / Tt`. Over the setup horizon `Ts`, the expected number of completions is

```
freeing_soon = C * (Ts / Tt)
```

Use `Ts` and not the tick interval because that is the horizon over which a call started
*now* will actually need an agent.

**Step 2 — demand already in the air.**
Calls that are already ringing will consume agents before ours do:

```
expected_answers_from_ringing = R * p
```

**Step 3 — net capacity available to new calls.**

```
capacity = A + freeing_soon - expected_answers_from_ringing
```

**Step 4 — invert the answer rate.**
To land `capacity` answered calls, start `capacity / p` calls:

```
n = max(0, floor(capacity / p))
```

### 10.3 Worked example (put this in the README)

`A=10, C=40, R=6, p_lb=0.25, Ts=3s, Tt=120s`

```
freeing_soon = 40 * (3/120)          = 1.0
expected_from_ringing = 6 * 0.25     = 1.5
capacity = 10 + 1.0 - 1.5            = 9.5
n = floor(9.5 / 0.25)                = 38     <- pacing engine REQUESTS 38
```

Safety controller, `max_overdial_ratio = 1.5`:

```
ceiling = floor(10 * 1.5)            = 15
allowed = 15 - agent_bound_inflight(6) - ringing(6) = 3
approved = 3, reason = OVERDIAL_CAP
```

**The system dials 3, not 38.** That gap is the entire point of the assignment, and the
`pacing_decisions` row proves both numbers and the reason.

### 10.4 Why a lower bound instead of the point estimate

Two properties, both defensible in one sentence each:

- **Cold start is safe automatically.** With `n = 0` samples the bound returns 0.05, so
  `n = capacity / 0.05` looks large — but the `INSUFFICIENT_SAMPLES` rule forces progressive
  until 30 samples exist, and past that the bound is still well below the point estimate, so
  the campaign warms up gradually instead of over-dialing on three lucky answers.
- **Optimistic noise cannot cause abandonment.** Over-estimating `p` is the dangerous error
  (you start too few calls — mild under-utilization). *Under*-estimating `p`... wait, invert
  it carefully: `n = capacity / p`, so a **smaller** `p` produces a **larger** `n`. This is
  the trap in the formula, and here is the resolution:

> **Use the lower bound in the `expected_answers_from_ringing` term (be pessimistic about
> capacity already consumed) and the UPPER bound in the divisor (be pessimistic about how
> many of our new calls will land).** Concretely, compute
> `n = floor(capacity_conservative / p_upper)` where
> `capacity_conservative = A + freeing_soon − R·p_upper`.
> Using the upper bound as the divisor means "assume more of my calls than expected will
> connect", which is the safe direction. Implement `WilsonBound.upper()` alongside
> `lower()` — same formula with `centre + margin` — and use `upper()` in `predictive.py`.

Fix `predictive.py` accordingly:

```python
p_hi = max(0.01, min(1.0, s.answer_rate_ub))    # pessimistic: assume high connect rate
freeing = s.calls_connected * (s.avg_setup_s / max(s.avg_talk_s, 1e-6))
capacity = s.agents_available + freeing - s.calls_ringing * p_hi
n = max(0, math.floor(capacity / p_hi))
```

Log **both** bounds and the point estimate in the rationale, and be ready to explain the
direction of the inequality. An interviewer who spots the inversion and finds you already
handled it learns more about you than any other line of code in the submission.

### 10.5 Reacting to a 70% → 10% collapse

Three independent mechanisms, in increasing order of severity:

1. **Asymmetric EWMA** on the raw answer rate: `min(fast, slow)` drops within a few seconds
   but recovers over minutes. Prevents a lucky burst from re-inflating pacing.
2. **Widening confidence interval.** When the rate shifts, recent variance rises, the Wilson
   interval widens, `p_upper` rises, `n` falls.
3. **Abandon budget circuit.** If reality outruns both estimators and abandonment crosses 3%,
   `s5_abandon_budget` trips a cooldown that forces literal progressive behaviour for 10
   seconds. This is a closed-loop control on the *outcome*, not on the prediction — it works
   even if every estimator is wrong.

---

## 11. Safety controller specification

### 11.1 Rule table

| # | Rule | Trigger | Effect | Reason code |
|---|---|---|---|---|
| S0 | Campaign active | campaign paused/inactive | 0 | `CAMPAIGN_PAUSED` |
| S1 | No agents | `agents_available == 0` | 0 | `NO_AGENTS` |
| S2 | Cooldown | breach within last 40 ticks | progressive cap | `COOLDOWN_FORCED_PROGRESSIVE` |
| S3 | Sample floor | `< 30` answer observations | progressive cap | `INSUFFICIENT_SAMPLES` |
| S4 | Provider health | best score `< 0.6` / all circuits open | scale by score / 0 | `PROVIDER_DEGRADED` |
| S5 | Abandon budget | 5-min abandon rate `> 3%` | progressive cap + trip cooldown | `ABANDON_BUDGET_BREACH` |
| S6 | Overdial cap | in-flight + n `> A × 1.5` | reduce to headroom | `OVERDIAL_CAP` |
| S7 | Ringing hard cap | `ringing + n > 500` | reduce to headroom | `RINGING_HARD_CAP` |

All rules evaluate; the minimum wins; every binding rule is recorded.

### 11.2 The four allowed outcomes (assignment requirement)

| Outcome | How it appears |
|---|---|
| Approve | `approved == requested`, reason `OK` |
| Reduce | `0 < approved < requested`, reason names the binding rule |
| Reject | `approved == 0` |
| Fall back to progressive | `approved == available − agent_bound_inflight`, reason `*_FORCED_PROGRESSIVE` / `INSUFFICIENT_SAMPLES` |

### 11.3 Proof of non-bypassability

Three layers. State all three; the third is the one that convinces.

1. **Type-level:** the engine's inputs and outputs are frozen dataclasses containing only
   numbers. No capability is reachable from them.
2. **Wiring-level:** only `SafetyController.__init__` receives the allocator.
3. **Test-level:** `test_safety_boundary.py` walks the AST of every module under `pacing/`
   and asserts no import of `allocator`, `providers`, or `repo`. CI fails if a future
   developer wires a shortcut. *This test is the answer to "how do you know it stays true?"*

---

## 12. Failure scenarios

Each is a runnable demo. `make sim SCENARIO=<x>` plus the assertion to point at.

### 12.1 Worker crash mid-dial

**Setup:** `agent RESERVED → borrower LOCKED → call INITIATED → SIGKILL the worker.`

**What happens:**
- Provider events for that call keep arriving at the API process and land in the ledger.
  Nothing is lost — ingest is decoupled from application.
- Another worker's `event_applier` picks them up (`applied=false` + `SKIP LOCKED`) and drives
  the call forward normally. **In the happy path, the crash is invisible.**
- If the call never generates events (crash happened before the provider accepted), the
  reaper fires at lease expiry: call `FAILED(LEASE_EXPIRED_NO_EVENTS)`, agent `AVAILABLE`,
  borrower rescheduled with backoff and `attempt_count += 1`.
- On restart, the dead worker retries its in-memory job. `UNIQUE(idempotency_key)` on
  `campaign:borrower:attempt` rejects the duplicate insert. **No double-dial.**

**Recovery bound:** worst case = `call_setup_lease_s` (45 s) + one reaper tick (1 s).

### 12.2 Provider outage

**Setup:** inject `ProviderOutage(at=600s, dur=120s)` on Mock B.

| Concern | Behaviour |
|---|---|
| Existing calls | Left alone; their leases expire and the reaper fails them, scheduling a provider-side cancel so we never leak a live call |
| New calls | Registry routes to Provider A (`score` still high) |
| If **all** providers open | S4 returns 0, dialing halts entirely — correct: better idle than abandoned |
| Retries | Exponential backoff on the borrower (`next_eligible_at`), not on the call. Retrying the *call* would double-dial; retrying the *borrower* is idempotent by attempt number |
| Pacing | `provider_health` enters the snapshot, so S4 scales the budget by health score before the outage even becomes an error rate |

**Show:** the chart where `approved` collapses to zero at t=600 with reason
`PROVIDER_DEGRADED`, then recovers via half-open probe at t=735.

### 12.3 Agent availability collapse (100 → 60 in seconds)

**Reaction time is bounded by three things:**

1. Agents going `OFFLINE` update `agents` immediately (logout path) — the counters change
   in the same transaction.
2. The next pacing tick (≤ 250 ms) reads the new `agents_available` and S6's ceiling
   `A × 1.5` drops from 150 to 90 instantly.
3. Calls already `RINGING` that now exceed capacity are **cancelled before answer** by the
   pullback sweep. Cancelling a ringing call has zero compliance cost — this is the free
   option predictive dialing gives you, and using it is what separates a real dialer from
   a naive one.

**Worst-case exposure:** calls that answer inside the 250 ms window between the drop and the
next tick. Those are absorbed by the overdial headroom, and if they cannot be, they count
against the abandon budget, which trips S5. **Bounded, measured, and logged at every layer.**

**Show:** `F_agentdrop` chart — utilization spikes, `approved` drops within one tick,
`calls_cancelled` rises, abandonment stays under the 3% line.

### 12.4 Duplicate events

`ANSWERED × 3 + COMPLETED` → `provider_events` holds 4 rows if the provider used 4 distinct
event ids, but only distinct *ids* insert; a genuinely re-delivered event (same id) is a
zero-row insert and returns 200 immediately. If the provider re-sends `ANSWERED` with a *new*
id, the rank guard rejects it (rank 4 not > rank 4) with `anomaly='OUT_OF_ORDER'`.
**Both duplication modes are covered — id-level and content-level.** One state transition,
one agent bridge.

### 12.5 Out-of-order events

`COMPLETED, ANSWERED, RINGING` → final state `COMPLETED`, two ledger rows marked
`OUT_OF_ORDER`, zero illegal transitions. The system does not break; it records the anomaly
and moves on. Run `SELECT event_type, anomaly FROM provider_events WHERE call_id=$1 ORDER BY id;`
in the demo — the audit trail *is* the answer.

---

## 13. Simulation plan

### 13.1 What the simulator actually simulates

- **Agents:** login/logout schedules, wrap-up durations, mid-call drops.
- **Borrowers:** answer/no-answer draws from the scenario's answer rate, talk-time draws
  from a lognormal centred on the scenario's mean.
- **Providers:** the mock behaviour matrix (Phase 5) driven by the same `VirtualClock`.
- **Everything else is real code.** The simulator injects a clock and providers; the
  allocator, safety controller, repos, and Postgres are the production path. This matters:
  a simulator that stubs out the DB would prove nothing about the concurrency claims.

### 13.2 Determinism

Every scenario takes a seed. Same seed → identical CSV, byte for byte. State this in the
README and commit the seeds used for the committed results, so a reviewer can reproduce
your exact charts.

### 13.3 The comparison that sells the submission

Run scenario B twice — once `PROGRESSIVE`, once `PREDICTIVE` — same seed. Report:

```
                     progressive   predictive   delta
agent utilization         44.1%        71.8%    +27.7pp
calls connected/hour        612          981      +60%
abandonment rate           0.00%        1.4%     within 3% budget
safety interventions          0          317     (of 1,240 ticks)
```

That table, with the abandonment column staying under budget, is the answer to the entire
assignment in five lines.

---

## 14. Testing strategy

| Layer | What | Tool |
|---|---|---|
| Unit | transition table, Wilson bounds, EWMA, breaker | pytest |
| Concurrency | I1, I2 under 50 racing tasks against real Postgres | pytest-asyncio |
| Property | any event permutation → same terminal state | hypothesis |
| Boundary | AST assertion that pacing cannot import allocator | pytest + `ast` |
| Recovery | kill-mid-dial → reaper → consistent state | pytest + VirtualClock |
| Scenario | full sim runs, assert invariants hold at every tick | sim harness |

**Invariant checking inside the simulation.** After every tick, assert:

```python
assert counters.agents_reserved + counters.agents_dialing <= agents_total
assert inflight_agent_bound <= agents_available_at_decision * limits.abs_max_overdial_ratio
assert abandon_rate_5m <= 0.05          # absolute ceiling, not the 3% target
assert no_call_has_rank_decreased()
```

A simulation that also acts as a 30-minute invariant fuzz test is worth more than either
piece alone. Say that in the README.

---

## 15. Load test and scale analysis

### 15.1 100 → 1,000 → 10,000 agents: what breaks, in order

**First break — the pacing snapshot query (~1,000 agents).**
`SELECT count(*) FROM agents WHERE campaign_id=$1 GROUP BY state` plus the equivalent over
`calls` runs every 250 ms. At 1,000 agents and ~1,500 in-flight calls those are sequential
scans over the hot indexes, and the tick starts missing its deadline. *Diagnosis: pacing tick
p95 exceeds 250 ms while allocation latency is still flat.*
**Fix:** `campaign_counters` — a single row per campaign maintained incrementally inside the
same transaction as each state change. Snapshot becomes a single-row primary-key read, O(1)
regardless of agent count. Cost: one extra hot-row `UPDATE` per state change, which is why
it is a Phase 12 optimisation and not a Phase 3 default.

**Second break — hot-row contention on `campaign_counters` (~3,000 agents).**
The fix above creates a single row every transaction updates, serialising writes.
**Fix:** shard the counter row `(campaign_id, shard_id)` across 16 shards chosen by
`agent_id % 16`; the snapshot sums 16 rows, still O(1) in agent count. Standard
counter-sharding, and worth naming as such.

**Third break — provider webhook ingest (~10,000 agents).**
This is the real one, and naming it is what separates a good answer from a generic one:
**provider events, not calls, are the volume driver.** At ~6 events per call and ~2 calls per
agent-hour, 10,000 agents generate roughly 33 event-inserts per second sustained, with bursts
several times that. Each is an individual `INSERT … ON CONFLICT` in its own transaction.
**Fix, in order:** (a) batch ingest — buffer 50 ms of webhooks and insert with a single
multi-row statement or `COPY`, amortising the fsync; (b) partition `provider_events` by day
and drop old partitions instead of deleting rows; (c) *only now* is a durable queue in front
of ingest justified. Say explicitly: I deliberately did not add a broker at 100 agents,
because it would have been complexity solving a problem I did not have.

**Fourth break — single Postgres write node (~10,000+ agents).**
**Fix:** campaigns are a natural shard key — they share no agents or borrowers. Route
campaigns to separate database clusters with a directory table. Cross-campaign agent pooling
is the thing that would break this, and if the business needs it, the shard key becomes the
agent pool rather than the campaign.

### 15.2 What does *not* break, and why say so

Agent reservation itself does not become the bottleneck, because contention is proportional
to `workers × tick_rate`, not to agent count — and `SKIP LOCKED` means workers never queue
behind each other for the same rows. Adding agents makes the candidate pool *larger*, which
*reduces* CAS collision rate. Pointing out the thing that scales fine demonstrates you
actually reasoned about it rather than listing generic bottlenecks.

### 15.3 The answer they do not want

Not "add more servers." The answer is: measure the tick deadline miss rate, identify the
aggregate scan, replace it with an O(1) counter read, then shard the counter, then batch
ingest, then shard by campaign — each step justified by a specific measured symptom.

---

## 16. Architecture decision records

Write each as five sentences: **Context / Decision / Consequence / What it makes harder /
What would change my mind.**

### ADR-0001 — PostgreSQL only, no Redis, no Kafka
Every hard requirement is mutual exclusion or idempotency, both of which Postgres provides
transactionally. Introducing Redis would create the cache-vs-DB divergence the assignment
explicitly probes; introducing Kafka would add at-least-once delivery I would still dedupe in
Postgres. **Makes harder:** single write node caps throughput (§15). **Would change my mind:**
sustained event ingest above ~5k/s, or a hard requirement for sub-millisecond snapshot reads.

### ADR-0002 — The safety boundary is structural, not procedural
The pacing engine is a pure function whose input and output types carry no capabilities, and
an AST test enforces that it cannot import the allocator. **Makes harder:** the engine cannot
do adaptive lookups mid-decision; everything it needs must be in the snapshot.
**Would change my mind:** nothing — this is the assignment's central requirement.

### ADR-0003 — Lease-based crash recovery instead of distributed locks or heartbeats
Every claim carries an expiry; a 1 Hz reaper reconciles anything past it. **Makes harder:**
recovery latency is bounded by the lease duration, not instant; short leases risk reaping
live work, so lease length is a real tuning parameter (30 s reserve, 45 s dial).
**Would change my mind:** a requirement for sub-second failover, which would need heartbeats
plus fencing tokens.

### ADR-0004 — Monotonic rank for ordering instead of provider timestamps
Provider clocks drift and B's timestamps are unreliable by construction. A rank enforced in
the SQL `WHERE` clause makes ordering a database invariant rather than application logic.
**Makes harder:** legitimate backward transitions are impossible (there are none in this
domain, but a REJOIN/HOLD feature would need rank redesign or a sub-state field).

### ADR-0005 — Virtual clock injected everywhere
A 30-minute campaign runs in ~2 seconds, tests are deterministic under a seed, and failures
are replayable. **Makes harder:** every `sleep` must route through the abstraction; enforced
by a lint test that greps for `asyncio.sleep` outside `clock.py`.

### ADR-0006 — Wilson bound over a point estimate for the answer rate
Cold campaigns pace conservatively without a special case, and the interval widens
automatically when behaviour shifts. **Makes harder:** slightly lower utilization in a stable
high-answer-rate campaign than a point estimate would give. **Would change my mind:** enough
historical per-segment data to justify a proper Bayesian prior per borrower cohort.

---

## 17. Interview defence sheet

Rehearse these out loud. One paragraph each, no hedging.

| Question | One-line spine of the answer |
|---|---|
| Two workers reserve the same agent | Conditional `UPDATE` on `(state, version)`; loser gets zero rows. No read-then-write exists in the code path. §4.3 |
| DB says AVAILABLE, cache says RESERVED — which wins? | The DB, by construction: the cache is never consulted for a mutating decision. Being wrong only changes *how many* I request. §7 |
| ANSWERED, crash, then COMPLETED | The event was durable before processing; another worker replays from the ledger; rank guard lands on COMPLETED; lease + reaper covers the no-event case. §5.4 |
| Predicted 70%, dropped to 10% | Three layers: asymmetric EWMA falls in seconds, the widening interval raises `p_upper`, and the abandon-budget circuit forces progressive on measured outcome regardless of prediction. §10.5 |
| 1,000 → 100,000 agents, what breaks first? | The pacing snapshot aggregate, then hot-counter contention, then webhook ingest — events, not calls, are the volume driver. §15 |
| Why 17 and not 10? | Read the `pacing_decisions` row: every input, every intermediate, every binding rule, verbatim. §10.3 |
| What are you least confident about? | *(Answer honestly — see below.)* |

**The "least confident" answer** — do not deflect this one, they are testing self-awareness:

> The `DIALING` → agent-release path. A `RESERVED` agent whose lease expires is trivially safe
> to reclaim, but a `DIALING` agent may have a live provider call, so I release it only via
> the call's terminal transition. That makes correctness depend on either an event arriving or
> the orphan sweep succeeding, and if a provider both stops sending events *and* fails the
> cancel API, an agent can stay stuck in `DIALING` until manual intervention. With another
> week I would add a periodic reconciliation poll against each provider's call-status endpoint
> for any call with no event in 60 seconds, which converts that from a stuck state into a
> bounded one.

**"What would you do differently with another week?"**
Provider status reconciliation polling (above); per-cohort answer-rate models instead of one
campaign-wide estimator; the counter-sharding from §15.1; and a proper abandonment definition
aligned to a specific regulatory standard rather than a generic 3%.

---

## 18. Final question answer

> **How would you build a SmartDialer that gets as much of the utilization benefit of
> predictive dialing as possible, while retaining the deterministic safety characteristics
> of progressive dialing?**

Keep this to ~250 words. Draft:

---

Progressive dialing is safe because of one invariant: every call in flight has an agent
committed to it. I would not abandon that invariant to get predictive utilization — I would
*widen* what counts as a committed agent.

Every call, in either mode, must hold an agent slot before it is placed. In progressive mode
the slot is backed by an agent who is idle right now. In predictive mode the slot may also be
backed by an agent *projected* to become free within the call-setup window — but the
projection uses a statistical **upper** bound on how many of my in-flight calls will connect
and a **lower** bound on how many agents will free up, so I only over-dial into capacity I can
defend probabilistically. The 1:1 accounting never disappears; the definition of "1" becomes
predictive, and every unit of prediction is bounded by a confidence interval rather than a
point estimate.

Two things then close the residual gap. First, ringing calls are cancellable at zero
compliance cost, so the instant the projection degrades — agents log out, answer rate spikes,
a provider stalls — I cancel the excess *before* anyone picks up. That is the free option
predictive dialing gives you, and it converts a prediction error into a wasted dial instead of
an abandoned customer. Second, measured abandonment feeds back as a control signal that shrinks
the over-dial budget automatically and, past a threshold, pins the system to literal
progressive behaviour for a cooldown.

The result is that utilization becomes a continuously tunable dial that degrades gracefully to
progressive under uncertainty — not a mode you switch into and hope.

---

## 19. README content

The README is graded (5%) and read first. Structure:

```markdown
# SmartDialer

Progressive + predictive outbound dialer with a non-bypassable safety boundary.

## Quick start
    cp .env.example .env
    make db          # postgres + migrations
    make seed        # 100 agents, 5000 borrowers
    make api &       # webhook receiver on :8000
    make run         # dialer worker (run 2-3 for the distributed demo)

## Run the simulations
    make sim                          # all scenarios -> loadtest/results/
    python -m smartdialer.cli sim --scenario D --seed 42

## Results
[utilization chart]  [requested-vs-approved chart]
| scenario table from §13.3 |

## The five things to look at
1. `repo/agents.py::RESERVE_CAS`      — how two workers cannot reserve one agent
2. `domain/transitions.py::can_apply` — how out-of-order and duplicate events die
3. `safety/controller.py`             — the boundary, and why it cannot be switched off
4. `pacing/predictive.py::decide`     — the arithmetic, logged verbatim every tick
5. `worker/reaper.py`                 — what happens when a worker dies mid-dial

## Invariants (with the test that proves each)
| table from §7 |

## Failure demos
    make sim SCENARIO=E_outage        # provider outage
    make sim SCENARIO=F_agentdrop     # 40 agents vanish
    pytest tests/test_crash_recovery.py -v
    pytest tests/test_out_of_order.py -v

## Why this stack
[the paragraph from §2.2]

## What breaks at scale
[the ordered list from §15.1]

## Architecture
See ARCHITECTURE.md and docs/adr/.
```

**Answer these four questions explicitly in the README** — the assignment asks them directly:

1. What did you choose? → §2.1
2. Why did you choose it? → §2.2
3. What problem does it solve? → §7 invariants
4. What does it make harder? → §2.3 and §15

---

## Appendix A — Checklist against the submission requirements

| Required | Where | Done |
|---|---|---|
| Working source code | `src/smartdialer/` | ☐ |
| README with setup | `README.md` §19 | ☐ |
| Architecture diagram | §3.2 → `docs/diagrams/` | ☐ |
| Agent state machine | §4.2 | ☐ |
| Call state machine | §5.2 | ☐ |
| Progressive dialer | Phase 6 | ☐ |
| Predictive pacing engine | Phase 7 | ☐ |
| Safety controller | Phase 8 | ☐ |
| Mock telecom providers (≥2, different) | Phase 5 | ☐ |
| Tests | Phase 11 | ☐ |
| Basic simulation | Phase 10 | ☐ |
| Basic load test | Phase 12 | ☐ |
| Architecture decision document | §16 → `docs/adr/` | ☐ |
| Final question answer | §18 → `docs/final-answer.md` | ☐ |
| Worker crash demo | §12.1 | ☐ |
| Provider outage demo | §12.2 | ☐ |
| Agent drop demo | §12.3 | ☐ |
| Duplicate event demo | §12.4 | ☐ |
| Out-of-order demo | §12.5 | ☐ |
| Scale reasoning | §15 | ☐ |
| Plivo integration (optional bonus) | `providers/plivo.py` | ☐ |

## Appendix B — Things that will lose marks

- A dashboard instead of tests. Explicitly de-prioritised by the rubric.
- Any `if DEBUG: skip_safety` escape hatch, however well-intentioned.
- Retrying the *call* on provider failure instead of the *borrower* — that is a double-dial.
- Holding a database transaction open across the provider network call.
- `SELECT … then UPDATE` anywhere in the allocation path.
- Kafka, Redis, or microservices added without a measured problem they solve.
- Charts with no baseline. Predictive utilization means nothing without the progressive run
  at the same seed next to it.
- Being unable to explain a line of generated code. The assignment says this explicitly:
  you do not need to have typed it, you do need to own it.
