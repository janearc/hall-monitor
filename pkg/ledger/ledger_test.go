package ledger

import (
	"testing"
	"time"
)

func TestSilenceAgainstOwnCadence(t *testing.T) {
	l := New()
	t0 := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)

	// steady 15s heartbeat, ten observations
	for i := range 10 {
		l.Observe("observability.events", t0.Add(time.Duration(i)*15*time.Second))
	}
	last := t0.Add(9 * 15 * time.Second)

	// quiet but within 3 cadences: not silent
	if s := l.Silent(last.Add(30 * time.Second)); len(s) != 0 {
		t.Fatalf("silence called early: %+v", s)
	}
	// past 3 cadences: silent, cited against its own history
	s := l.Silent(last.Add(50 * time.Second))
	if len(s) != 1 || s[0].Topic != "observability.events" {
		t.Fatalf("silence missed: %+v", s)
	}
	if s[0].ExpectedCadence != 15*time.Second {
		t.Fatalf("cadence = %v, want 15s", s[0].ExpectedCadence)
	}
}

func TestNoHistoryNoVerdict(t *testing.T) {
	l := New()
	t0 := time.Now()
	// three observations = two gaps, below the minGaps floor
	for i := range 3 {
		l.Observe("young.topic", t0.Add(time.Duration(i)*time.Second))
	}
	if s := l.Silent(t0.Add(time.Hour)); len(s) != 0 {
		t.Fatalf("verdict without history: %+v", s)
	}
	if _, ok := l.Cadence("young.topic"); ok {
		t.Fatal("cadence stated without history")
	}
}

func TestMedianResistsOutage(t *testing.T) {
	l := New()
	t0 := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	at := t0
	// steady 10s gaps with one 10-minute outage in the middle
	for i := range 12 {
		if i == 6 {
			at = at.Add(10 * time.Minute)
		} else {
			at = at.Add(10 * time.Second)
		}
		l.Observe("bursty.topic", at)
	}
	cadence, ok := l.Cadence("bursty.topic")
	if !ok || cadence != 10*time.Second {
		t.Fatalf("median cadence = %v ok=%v, want 10s (outage must not teach the cadence)", cadence, ok)
	}
}
