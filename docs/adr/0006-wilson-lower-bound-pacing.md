# ADR-0006 — Wilson bound over a point estimate for the answer rate

**Context.** Predictive pacing needs an estimate of the answer probability
`p`. A raw point estimate (`answered / attempted`) is dangerously noisy with
few samples, and the pacing formula's sensitivity to `p` is asymmetric: since
`n = capacity / p`, a *smaller* `p` actually produces a *larger* `n` — the
naive intuition ("be pessimistic, use a low p") is backwards for the divisor.

**Decision.** Estimate `p` with a Wilson score interval instead of a point
estimate, and use the **upper** bound (`p_ub`) — not the lower bound — as the
divisor and in the "demand already in the air" term, because assuming *more*
of our calls will connect than they probably will is the safe direction under
that inversion. With zero samples the interval defaults to a wide range that
paces like progressive dialing; `INSUFFICIENT_SAMPLES` (safety rule S3) forces
literal progressive behaviour below 30 observations regardless.

**Consequence.** Cold campaigns warm up gradually instead of over-dialing on a
few lucky early answers, and the interval automatically widens (raising
`p_ub`, lowering `n`) the moment recent behaviour gets noisier — before the
abandon-budget circuit would ever need to trip.

**What it makes harder.** Slightly lower utilization in a stable,
high-answer-rate campaign than a point estimate would allow, because the
upper bound is always ≥ the point estimate.

**What would change my mind.** Enough historical per-segment data to justify a
proper Bayesian prior per borrower cohort instead of one campaign-wide
estimator.
