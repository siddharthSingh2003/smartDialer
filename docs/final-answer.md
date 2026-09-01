# Final question

> How would you build a SmartDialer that gets as much of the utilization
> benefit of predictive dialing as possible, while retaining the deterministic
> safety characteristics of progressive dialing?

Progressive dialing is safe because of one invariant: every call in flight has
an agent committed to it. I would not abandon that invariant to get predictive
utilization — I would *widen* what counts as a committed agent.

Every call, in either mode, must hold an agent slot before it is placed. In
progressive mode the slot is backed by an agent who is idle right now. In
predictive mode the slot may also be backed by an agent *projected* to become
free within the call-setup window — but the projection uses a statistical
**upper** bound on how many of my in-flight calls will connect and an implicit
**lower** bound on how many agents will free up, so I only over-dial into
capacity I can defend probabilistically. The 1:1 accounting never disappears;
the definition of "1" becomes predictive, and every unit of prediction is
bounded by a confidence interval rather than a point estimate.

Two things then close the residual gap. First, ringing calls are cancellable
at zero compliance cost, so the instant the projection degrades — agents log
out, answer rate spikes, a provider stalls — I cancel the excess *before*
anyone picks up. That is the free option predictive dialing gives you, and it
converts a prediction error into a wasted dial instead of an abandoned
customer. Second, measured abandonment feeds back as a control signal that
shrinks the over-dial budget automatically and, past a threshold, pins the
system to literal progressive behaviour for a cooldown.

The result is that utilization becomes a continuously tunable dial that
degrades gracefully to progressive under uncertainty — not a mode you switch
into and hope.

This build implements exactly that: `pacing/progressive.py` is the 1:1 rule;
`pacing/predictive.py` is the same rule with a projected slot count; the
overdial ceiling in `safety/rules.py::s6_overdial_cap` bounds how far the
projection is trusted; `allocator/allocator.py::cancel_excess_ringing` is the
free-cancellation pullback; and `safety/rules.py::s5_abandon_budget` is the
measured-outcome control loop that forces progressive behaviour when reality
disagrees with the projection, regardless of what the estimators believe.
