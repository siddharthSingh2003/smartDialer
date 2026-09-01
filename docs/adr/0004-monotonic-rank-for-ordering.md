# ADR-0004 — Monotonic rank for ordering instead of provider timestamps

**Context.** Provider webhooks arrive out of order and sometimes duplicated;
Mock Provider B is built specifically to do both. Provider-supplied
timestamps cannot be trusted to order events correctly across providers or
even within one flaky one.

**Decision.** Every call state has a fixed integer rank (`domain/transitions.py
::RANK`). A transition is applied only if it is both forward (`incoming rank >
current rank`) and legal (`incoming ∈ LEGAL_NEXT[current]`), enforced in the
database's `WHERE state_rank < $3` predicate on the same statement that writes
the new state — not as a separate check-then-write in application code. Any
terminal state (`COMPLETED`/`FAILED`/`CANCELLED`/`ABANDONED`) is legal from any
non-terminal state, because a provider that drops an intermediate webhook
still needs its terminal event honoured rather than rejected as illegal.

**Consequence.** Ordering becomes a database invariant instead of application
logic: two workers advancing the same call concurrently cannot interleave into
an inconsistent state, because the row lock serialises them and the loser's
predicate simply fails.

**What it makes harder.** Legitimate backward transitions are impossible by
construction. There are none in this domain today, but a future REJOIN/HOLD
feature would need either a rank redesign or a separate sub-state field rather
than fitting into the existing rank.

**What would change my mind.** A requirement to model a call re-entering an
earlier stage (e.g. warm transfer back to IVR) — the current rank model
assumes monotonic progress toward a terminal outcome.
