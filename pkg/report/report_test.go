package report

import (
	"testing"
	"time"

	"github.com/janearc/hall-monitor/pkg/ledger"
)

type fakeSource struct {
	producers map[string]time.Time
	groups    map[string][]string
}

// Snapshot returns the fake's fixed state.
func (f fakeSource) Snapshot() (map[string]time.Time, map[string][]string) {
	return f.producers, f.groups
}

func TestBuildFlagsVoidAndSilent(t *testing.T) {
	t0 := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)

	led := ledger.New()
	// delight.events: steady 15s cadence, then goes quiet
	for i := range 10 {
		led.Observe("delight.events", t0.Add(time.Duration(i)*15*time.Second))
	}
	lastBeat := t0.Add(9 * 15 * time.Second)

	src := fakeSource{
		producers: map[string]time.Time{
			"delight.events":          lastBeat,
			"observability.transfers": lastBeat, // the void: nobody listens
		},
		groups: map[string][]string{
			"delightd": {"delight.events"},
		},
	}

	now := lastBeat.Add(2 * time.Minute) // far past 3x 15s cadence
	r := Build(src, led, now)

	if len(r.Topics) != 2 {
		t.Fatalf("topics = %d, want 2", len(r.Topics))
	}
	byTopic := map[string]TopicRow{}
	for _, row := range r.Topics {
		byTopic[row.Topic] = row
	}
	if !byTopic["observability.transfers"].Void {
		t.Fatal("void not flagged")
	}
	if byTopic["delight.events"].Void {
		t.Fatal("consumed topic flagged void")
	}
	if !byTopic["delight.events"].Silent {
		t.Fatal("silence not flagged against own cadence")
	}

	// findings: one void (transfers), one silent (events), and transfers is
	// also silent-eligible only if it has ledger history (it does not)
	kinds := map[string]int{}
	for _, f := range r.Findings {
		kinds[f.Kind]++
		if f.Class != "refusal" {
			t.Fatalf("finding class = %q, want refusal", f.Class)
		}
	}
	if kinds["void"] != 1 || kinds["silent"] != 1 {
		t.Fatalf("findings = %+v, want 1 void + 1 silent", r.Findings)
	}
}

func TestBuildNilLedger(t *testing.T) {
	src := fakeSource{producers: map[string]time.Time{"a.topic": time.Now()}, groups: nil}
	r := Build(src, nil, time.Now())
	if len(r.Topics) != 1 || !r.Topics[0].Void {
		t.Fatalf("nil-ledger build wrong: %+v", r.Topics)
	}
}
