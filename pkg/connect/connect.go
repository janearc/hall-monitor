// Package connect is the bounded retry around hm's wire. One attempt at
// startup was the fault this package closes: on 2026-08-22 the cluster came
// back from a VM restart with hm scheduled before kafka's DNS existed, the
// single dial failed, and hm sat degraded for forty minutes with nothing in
// the process ever trying again. The wire is the only thing hm exists to
// watch, so losing it is a condition to recover from, not a state to report
// once and keep.
//
// The loop is bounded in INTERVAL, not in attempts: it tries until the
// context ends, sleeping a full-jitter exponential backoff between failures
// (base doubling to a cap, each sleep uniform in [0, current]). Unbounded
// attempts are correct here -- a hall monitor with no broker has nothing
// else to do -- and the cap keeps a long outage from turning into a tight
// loop against a broker that is trying to come up.
package connect

import (
	"context"
	"math/rand/v2"
	"time"
)

// Dial is one connection attempt. It returns nil when the wire is there.
type Dial func(ctx context.Context) error

// Policy is the backoff shape. Zero values take the defaults below.
type Policy struct {
	// Base is the first sleep's ceiling; each failure doubles it up to Cap.
	Base time.Duration
	// Cap bounds the ceiling, so a long outage polls at most this often.
	Cap time.Duration
	// Rand returns a uniform integer in [0, n). Tests pin it; nil is math/rand.
	Rand func(n int64) int64
}

// Defaults is the policy hm runs with: a second, doubling to a minute.
var Defaults = Policy{Base: time.Second, Cap: time.Minute}

// Attempt is what the loop tells its observer after every try: which
// attempt, what happened, and how long it will wait before the next one
// (zero on success or when the context ended).
type Attempt struct {
	N    int
	Err  error
	Next time.Duration
}

// Loop calls dial until it succeeds or ctx ends. observe is called after
// every attempt, success included, so the caller can count and log each
// transition; nil is allowed. Returns nil on success and ctx.Err() when the
// context ended first -- never a dial error, because a dial error is not a
// terminal condition here.
func Loop(ctx context.Context, p Policy, dial Dial, observe func(Attempt)) error {
	if p.Base <= 0 {
		p.Base = Defaults.Base
	}
	if p.Cap <= 0 {
		p.Cap = Defaults.Cap
	}
	if p.Cap < p.Base {
		p.Cap = p.Base
	}
	if p.Rand == nil {
		p.Rand = rand.Int64N
	}
	if observe == nil {
		observe = func(Attempt) {}
	}
	ceiling := p.Base
	for n := 1; ; n++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := dial(ctx)
		if err == nil {
			observe(Attempt{N: n})
			return nil
		}
		if ctx.Err() != nil {
			// the dial failed because we are shutting down; not an outage
			observe(Attempt{N: n, Err: err})
			return ctx.Err()
		}
		wait := sleepFor(ceiling, p.Rand)
		observe(Attempt{N: n, Err: err, Next: wait})
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		if ceiling < p.Cap {
			ceiling *= 2
			if ceiling > p.Cap {
				ceiling = p.Cap
			}
		}
	}
}

// sleepFor is full jitter: uniform in [0, ceiling). The floor is zero on
// purpose -- what matters for a fleet of retrying clients is that they do
// not agree on a moment, and a zero draw is just an early retry.
func sleepFor(ceiling time.Duration, r func(int64) int64) time.Duration {
	if ceiling <= 0 {
		return 0
	}
	return time.Duration(r(int64(ceiling)))
}
