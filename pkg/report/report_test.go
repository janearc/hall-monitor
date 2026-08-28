package report

import (
	"context"
	"encoding/binary"
	"github.com/janearc/big-little-mesh/frood"
	obspb "github.com/janearc/big-little-mesh/gen/go/observability/v1"
	"github.com/janearc/hall-monitor/pkg/lease"
	"google.golang.org/protobuf/proto"
	"log/slog"
	"testing"
	"time"

	"github.com/janearc/hall-monitor/pkg/ledger"
)

type fakeSource struct {
	offContract map[string]int64
	producers   map[string]time.Time
	groups      map[string][]string
}

// Snapshot returns the fake's fixed state.
func (f fakeSource) Snapshot() (map[string]time.Time, map[string][]string) {
	return f.producers, f.groups
}

func (f fakeSource) OffContract() (map[string]int64, map[string]time.Time) {
	return f.offContract, nil
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
	r := Build(src, led, nil, now)

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
	r := Build(src, nil, nil, time.Now())
	if len(r.Topics) != 1 || !r.Topics[0].Void {
		t.Fatalf("nil-ledger build wrong: %+v", r.Topics)
	}
}

// TestAuthorizedTableAndLeaseExpiryFinding exercises the lease seam: the
// authority's table rides the report, and an expired lease is a refusal-class
// finding beside the void and silent kinds.
func TestAuthorizedTableAndLeaseExpiryFinding(t *testing.T) {
	src := fakeSource{producers: map[string]time.Time{}, groups: map[string][]string{}}

	auth := lease.New(context.Background(), nil, slog.Default())
	t0 := time.Now().Add(-10 * time.Minute)
	auth.Observe(frood.TopicObservability, t0, framedBeat(t, "flipr"))
	auth.Observe(frood.TopicObservability, t0.Add(10*time.Second), framedBeat(t, "flipr"))
	auth.Tick(t0.Add(5 * time.Minute)) // silence well past three cadences

	r := Build(src, nil, auth, time.Now())
	if len(r.Authorized) != 1 || r.Authorized[0].State != "expired" {
		t.Fatalf("want one expired row, got %+v", r.Authorized)
	}
	found := false
	for _, f := range r.Findings {
		if f.Kind == "lease-expired" && f.Topic == "flipr" && f.Class == "refusal" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expired lease must be a refusal finding, got %+v", r.Findings)
	}

	// nil authority: no table, no lease findings, nothing breaks
	r = Build(src, nil, nil, time.Now())
	if r.Authorized != nil {
		t.Fatalf("nil authority must mean no table, got %+v", r.Authorized)
	}
}

// framedBeat builds a schema-registry-framed heartbeat, the wire shape the
// authority consumes: magic, schema id, single-message index, payload.
func framedBeat(t *testing.T, name string) []byte {
	t.Helper()
	payload, err := proto.Marshal(&obspb.ServiceHealthHeartbeat{ServiceName: name})
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 6, 6+len(payload))
	binary.BigEndian.PutUint32(buf[1:5], 1)
	return append(buf, payload...)
}
