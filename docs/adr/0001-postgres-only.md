# ADR-0001 — PostgreSQL only, no Redis, no Kafka

**Context.** Every hard requirement in this assignment reduces to mutual exclusion
(don't double-book an agent or a borrower) or idempotency (don't double-apply a
provider event). Both are things a relational database provides transactionally,
today, with a decade of correctness behind them.

**Decision.** Use a single PostgreSQL instance as the only stateful store. A
conditional `UPDATE` gives compare-and-swap agent reservation; `SELECT ... FOR
UPDATE SKIP LOCKED` gives lock-free work partitioning across N workers; a
`UNIQUE` constraint gives exactly-once provider-event application;
`pg_advisory_lock` gives leader election for the pacing loop.

**Consequence.** The queue, the lock manager, and the state store are one
system with one consistency model. There is no cache-vs-database divergence to
reason about, because there is no cache in the write path.

**What it makes harder.** A single write node caps throughput somewhere around
5,000-10,000 agents; see §15 of ARCHITECTURE.md for where it degrades first and
the fix order (O(1) counters, counter sharding, batched event ingest, then
campaign-keyed sharding).

**What would change my mind.** Sustained event ingest above ~5k/s, or a hard
requirement for sub-millisecond snapshot reads that a single Postgres primary
cannot deliver even with the O(1) counter table.
