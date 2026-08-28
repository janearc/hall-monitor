package lease

import (
	"context"
	"encoding/binary"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/janearc/big-little-mesh/frood"
	leasepb "github.com/janearc/big-little-mesh/gen/go/lease/v1"
	obspb "github.com/janearc/big-little-mesh/gen/go/observability/v1"
)

// fakePub records every emitted verdict, in order.
type fakePub struct{ verdicts []*leasepb.LeaseVerdict }

func (f *fakePub) Publish(_ context.Context, _, _, _, _ string, msg proto.Message) error {
	f.verdicts = append(f.verdicts, proto.Clone(msg).(*leasepb.LeaseVerdict))
	return nil
}

// frame wraps a message the way the schema-registry wire format does: magic
// byte, four-byte schema id, payload. StripFrame removes exactly this.
func frame(t *testing.T, msg proto.Message) []byte {
	t.Helper()
	payload, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	// magic + schema id + the single-message index byte (0x00) + payload --
	// the exact shape StripFrame documents as emit.encode's inverse.
	buf := make([]byte, 6, 6+len(payload))
	buf[0] = 0
	binary.BigEndian.PutUint32(buf[1:5], 1)
	buf[5] = 0
	return append(buf, payload...)
}

func beat(t *testing.T, name string) []byte {
	t.Helper()
	return frame(t, &obspb.ServiceHealthHeartbeat{ServiceName: name})
}

func testAuthority() (*Authority, *fakePub) {
	pub := &fakePub{}
	return New(context.Background(), pub, nil, slog.Default()), pub
}

func TestFirstBeatAuthorizesAndEmitsOnce(t *testing.T) {
	a, pub := testAuthority()
	at := time.Now()
	a.Observe(frood.TopicObservability, at, beat(t, "flipr"))

	rows := a.Snapshot()
	if len(rows) != 1 || rows[0].Service != "flipr" || rows[0].State != "authorized" {
		t.Fatalf("want one authorized flipr row, got %+v", rows)
	}
	if rows[0].CadenceMS != 0 || rows[0].ExpiresAt != nil {
		t.Fatalf("single beat must carry no cadence and no expiry: %+v", rows[0])
	}
	if len(pub.verdicts) != 1 || pub.verdicts[0].GetState() != leasepb.LeaseState_LEASE_STATE_AUTHORIZED {
		t.Fatalf("want exactly one AUTHORIZED verdict, got %+v", pub.verdicts)
	}
	// a second beat is a renewal, not a transition: no new verdict
	a.Observe(frood.TopicObservability, at.Add(10*time.Second), beat(t, "flipr"))
	if len(pub.verdicts) != 1 {
		t.Fatalf("renewal must not re-emit, got %d verdicts", len(pub.verdicts))
	}
}

func TestCadenceIsLearnedFromGaps(t *testing.T) {
	a, _ := testAuthority()
	t0 := time.Now()
	a.Observe(frood.TopicObservability, t0, beat(t, "kingfisher"))
	a.Observe(frood.TopicObservability, t0.Add(20*time.Second), beat(t, "kingfisher"))

	rows := a.Snapshot()
	if rows[0].CadenceMS != 20000 {
		t.Fatalf("first gap should set cadence: want 20000ms got %d", rows[0].CadenceMS)
	}
	if rows[0].ExpiresAt == nil {
		t.Fatal("with a cadence learned, expiry must be stated")
	}
	// a LONGER gap is adopted whole (grow fast): 40s replaces 20s outright
	a.Observe(frood.TopicObservability, t0.Add(60*time.Second), beat(t, "kingfisher"))
	if got := a.Snapshot()[0].CadenceMS; got != 40000 {
		t.Fatalf("grow-fast: want 40000ms got %d", got)
	}
	// a SHORTER gap shrinks slowly (rollout double-beats must not halve the
	// cadence): 0.7*40s + 0.3*10s = 31s
	a.Observe(frood.TopicObservability, t0.Add(70*time.Second), beat(t, "kingfisher"))
	if got := a.Snapshot()[0].CadenceMS; got != 31000 {
		t.Fatalf("shrink-slow: want 31000ms got %d", got)
	}
}

func TestSilenceDecaysThroughExpiringToExpired(t *testing.T) {
	a, pub := testAuthority()
	t0 := time.Now()
	a.Observe(frood.TopicObservability, t0, beat(t, "flipr"))
	a.Observe(frood.TopicObservability, t0.Add(10*time.Second), beat(t, "flipr"))
	pub.verdicts = nil // only the decay interests this test

	// at exactly one cadence past the beat: still authorized -- the margin
	// exists precisely so an exactly-learned cadence cannot flap
	a.Tick(t0.Add(20 * time.Second))
	if n := len(pub.verdicts); n != 0 {
		t.Fatalf("no transition inside one cadence, got %d verdicts", n)
	}
	// past the margin (1.5 cadences): expiring, one verdict
	a.Tick(t0.Add(26 * time.Second))
	if len(pub.verdicts) != 1 || pub.verdicts[0].GetState() != leasepb.LeaseState_LEASE_STATE_EXPIRING {
		t.Fatalf("want one EXPIRING verdict, got %+v", pub.verdicts)
	}
	// ticks are idempotent between transitions
	a.Tick(t0.Add(30 * time.Second))
	if len(pub.verdicts) != 1 {
		t.Fatalf("idempotent tick re-emitted: %d", len(pub.verdicts))
	}
	// past three cadences: expired, no expiry timestamp in the verdict
	a.Tick(t0.Add(41 * time.Second))
	if len(pub.verdicts) != 2 || pub.verdicts[1].GetState() != leasepb.LeaseState_LEASE_STATE_EXPIRED {
		t.Fatalf("want EXPIRED verdict, got %+v", pub.verdicts)
	}
	if pub.verdicts[1].GetExpiresAt() != nil {
		t.Fatal("EXPIRED must not state an expiry")
	}
	if a.Snapshot()[0].State != "expired" {
		t.Fatalf("table must read expired: %+v", a.Snapshot())
	}
}

func TestReturnFromTheDeadReauthorizes(t *testing.T) {
	a, pub := testAuthority()
	t0 := time.Now()
	a.Observe(frood.TopicObservability, t0, beat(t, "flipr"))
	a.Observe(frood.TopicObservability, t0.Add(10*time.Second), beat(t, "flipr"))
	a.Tick(t0.Add(60 * time.Second)) // expired
	pub.verdicts = nil

	a.Observe(frood.TopicObservability, t0.Add(70*time.Second), beat(t, "flipr"))
	if len(pub.verdicts) != 1 || pub.verdicts[0].GetState() != leasepb.LeaseState_LEASE_STATE_AUTHORIZED {
		t.Fatalf("a heartbeat after expiry re-authorizes with a verdict, got %+v", pub.verdicts)
	}
}

func TestObserveIgnoresWhatItMust(t *testing.T) {
	a, pub := testAuthority()
	at := time.Now()

	// wrong topic
	a.Observe("flipr.ops", at, beat(t, "flipr"))
	// unframed bytes
	a.Observe(frood.TopicObservability, at, []byte("not framed"))
	// framed but empty service name
	a.Observe(frood.TopicObservability, at, beat(t, ""))
	// a LeaseVerdict on the shared topic MUST NOT renew a lease: field 3's
	// wire type disagrees with the heartbeat's, so unmarshal fails. This is
	// the self-loop guard; if this test breaks, the discrimination broke.
	v := &leasepb.LeaseVerdict{ServiceName: "hm",
		State:         leasepb.LeaseState_LEASE_STATE_AUTHORIZED,
		LastHeartbeat: timestamppb.Now(), IssuedAt: timestamppb.Now()}
	a.Observe(frood.TopicObservability, at, frame(t, v))

	if len(a.Snapshot()) != 0 || len(pub.verdicts) != 0 {
		t.Fatalf("nothing should have registered: rows=%+v verdicts=%+v", a.Snapshot(), pub.verdicts)
	}
}

func TestNilPublisherJudgesWithoutEmitting(t *testing.T) {
	a := New(context.Background(), nil, nil, slog.Default())
	a.Observe(frood.TopicObservability, time.Now(), beat(t, "flipr"))
	if len(a.Snapshot()) != 1 {
		t.Fatal("judging must not depend on emission")
	}
}
