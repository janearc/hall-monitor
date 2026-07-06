// Package ledger is the absence ledger: per-topic presence with history, so
// that a missing result is a first-class finding instead of a mystery. It
// learns each topic's cadence from observed inter-arrival gaps and flags the
// topics that have gone silent against their own history — the RFC's
// "declared emission silent past N cadences", grounded in observed cadence
// until declared traffic exists as machine data.
package ledger

import (
	"sort"
	"sync"
	"time"
)

// gapWindow is how many inter-arrival gaps a topic's history keeps. Small on
// purpose: enough to find a stable median, cheap enough to hold for every
// topic on the bus.
const gapWindow = 16

// minGaps is the history floor below which silence is never called: a topic
// seen twice has no cadence, only coincidence.
const minGaps = 4

// silenceFactor is the N in "silent past N cadences".
const silenceFactor = 3

// entry is one topic's presence history.
type entry struct {
	lastSeen time.Time
	prev     time.Time
	gaps     [gapWindow]time.Duration
	n        int // gaps written (caps at gapWindow; ring index is n%gapWindow)
}

// Ledger accumulates observations. Safe for one writer (the watch loop) and
// concurrent readers (the truth report).
type Ledger struct {
	mu      sync.RWMutex
	entries map[string]*entry
}

// New returns an empty ledger.
func New() *Ledger {
	return &Ledger{entries: map[string]*entry{}}
}

// Observe records one record on topic at time at.
func (l *Ledger) Observe(topic string, at time.Time) {
	l.mu.Lock()
	e, ok := l.entries[topic]
	if !ok {
		e = &entry{}
		l.entries[topic] = e
	}
	if !e.prev.IsZero() {
		e.gaps[e.n%gapWindow] = at.Sub(e.prev)
		e.n++
	}
	e.prev = at
	if at.After(e.lastSeen) {
		e.lastSeen = at
	}
	l.mu.Unlock()
}

// Silence is one topic that has gone quiet against its own cadence.
type Silence struct {
	Topic string
	// LastRecord is when the topic last carried a record.
	LastRecord time.Time
	// ExpectedCadence is the observed median inter-arrival gap.
	ExpectedCadence time.Duration
	// SilentFor is how long the topic has exceeded its expectation, as of
	// the asking time.
	SilentFor time.Duration
}

// Silent returns the topics whose quiet exceeds silenceFactor times their
// observed median cadence, sorted by topic. Topics with fewer than minGaps
// observed gaps are never reported: no history, no verdict (refusal-default
// applies to hm's own claims too — it does not call silence it cannot cite).
func (l *Ledger) Silent(now time.Time) []Silence {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var out []Silence
	for topic, e := range l.entries {
		if e.n < minGaps {
			continue
		}
		cadence := medianGap(e)
		if cadence <= 0 {
			continue
		}
		quiet := now.Sub(e.lastSeen)
		if quiet > silenceFactor*cadence {
			out = append(out, Silence{
				Topic:           topic,
				LastRecord:      e.lastSeen,
				ExpectedCadence: cadence,
				SilentFor:       quiet,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Topic < out[j].Topic })
	return out
}

// Cadence returns the observed median inter-arrival gap for topic, and
// whether enough history exists to state one.
func (l *Ledger) Cadence(topic string) (time.Duration, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	e, ok := l.entries[topic]
	if !ok || e.n < minGaps {
		return 0, false
	}
	return medianGap(e), true
}

// medianGap computes the median of the recorded gaps. Median, not mean: one
// long outage in the window must not teach the ledger that outages are the
// cadence. Caller holds the lock.
func medianGap(e *entry) time.Duration {
	n := min(e.n, gapWindow)
	gaps := make([]time.Duration, n)
	copy(gaps, e.gaps[:n])
	sort.Slice(gaps, func(i, j int) bool { return gaps[i] < gaps[j] })
	return gaps[n/2]
}
