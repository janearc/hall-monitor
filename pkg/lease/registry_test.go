package lease

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/janearc/big-little-mesh/frood"
	leasepb "github.com/janearc/big-little-mesh/gen/go/lease/v1"
	obspb "github.com/janearc/big-little-mesh/gen/go/observability/v1"
	leaseproto "github.com/janearc/big-little-mesh/proto/lease/v1"
	obsproto "github.com/janearc/big-little-mesh/proto/observability/v1"
)

// fakeResolver answers from a fixed table, or fails wholesale -- the two
// registry states Observe dispatches on.
type fakeResolver struct {
	subjects map[int32]string
	err      error
}

func (f *fakeResolver) Subject(_ context.Context, id int32) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	s, ok := f.subjects[id]
	if !ok {
		return "", fmt.Errorf("no subject for id %d", id)
	}
	return s, nil
}

// frameID frames a message under a chosen schema id, optionally appending
// residue: a well-formed field this build never vendored (field 90, varint),
// which is what schema evolution looks like on the wire.
func frameID(t *testing.T, id uint32, msg proto.Message, residue bool) []byte {
	t.Helper()
	payload, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	if residue {
		payload = append(payload, 0xD0, 0x05, 0x07) // field 90, varint, value 7
	}
	buf := make([]byte, 6, 6+len(payload))
	buf[0] = 0
	binary.BigEndian.PutUint32(buf[1:5], id)
	buf[5] = 0
	return append(buf, payload...)
}

// TestRegistrySaysHeartbeat_ResidueTolerated is the property this change
// exists for: with the registry naming the subject, a heartbeat carrying a
// field this build has not vendored still RENEWS -- leases do not stall on
// schema evolution. The same bytes under no resolver are refused, which pins
// that the delta is the registry, not a loosened check.
func TestRegistrySaysHeartbeat_ResidueTolerated(t *testing.T) {
	evolved := frameID(t, 7, &obspb.ServiceHealthHeartbeat{ServiceName: "flipr"}, true)

	withRegistry := New(context.Background(), nil,
		&fakeResolver{subjects: map[int32]string{7: obsproto.SubjectServiceHealthHeartbeat}}, slog.Default())
	withRegistry.Observe(frood.TopicObservability, time.Now(), evolved)
	if rows := withRegistry.Snapshot(); len(rows) != 1 || rows[0].Service != "flipr" {
		t.Fatalf("registry-named heartbeat with residue must renew; rows=%+v", rows)
	}

	without := New(context.Background(), nil, nil, slog.Default())
	without.Observe(frood.TopicObservability, time.Now(), evolved)
	if rows := without.Snapshot(); len(rows) != 0 {
		t.Fatalf("residue without a registry must refuse (legacy check); rows=%+v", rows)
	}
}

// TestRegistrySaysVerdict_NotRenewed: hm must not renew a lease off its own
// verdict, and with the registry the refusal is by NAME, not residue.
func TestRegistrySaysVerdict_NotRenewed(t *testing.T) {
	v := frameID(t, 8, &leasepb.LeaseVerdict{ServiceName: "flipr", Authority: "hm"}, false)
	a := New(context.Background(), nil,
		&fakeResolver{subjects: map[int32]string{8: leaseproto.SubjectLeaseVerdict}}, slog.Default())
	a.Observe(frood.TopicObservability, time.Now(), v)
	if rows := a.Snapshot(); len(rows) != 0 {
		t.Fatalf("a verdict must never renew; rows=%+v", rows)
	}
}

// TestResolverOutage_FallsBackConservatively: registry silent means the
// legacy residue check judges -- a clean heartbeat renews, an evolved one is
// refused. Degraded, visible, never stalled.
func TestResolverOutage_FallsBackConservatively(t *testing.T) {
	down := &fakeResolver{err: fmt.Errorf("registry unreachable")}
	a := New(context.Background(), nil, down, slog.Default())

	a.Observe(frood.TopicObservability, time.Now(),
		frameID(t, 7, &obspb.ServiceHealthHeartbeat{ServiceName: "kingfisher"}, false))
	if rows := a.Snapshot(); len(rows) != 1 || rows[0].Service != "kingfisher" {
		t.Fatalf("clean heartbeat must renew through the fallback; rows=%+v", rows)
	}

	a.Observe(frood.TopicObservability, time.Now(),
		frameID(t, 7, &obspb.ServiceHealthHeartbeat{ServiceName: "dodo"}, true))
	if rows := a.Snapshot(); len(rows) != 1 {
		t.Fatalf("evolved heartbeat under an outage must be refused, conservatively; rows=%+v", rows)
	}
}
