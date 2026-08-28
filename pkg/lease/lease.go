// Package lease is hm's runtime lease authority: the operator's "visible at a
// glance who is authorized to be on the network", judged from the wire.
//
// THE HEARTBEAT IS THE RENEWAL. There is no request and no grant: a service's
// observability.v1.ServiceHealthHeartbeat is its keepalive (docs/frood.md), and
// this package judges presence into AUTHORIZED, EXPIRING or EXPIRED against
// each service's OWN observed cadence -- three missed cadences is expiry, the
// same rule the absence ledger applies to topics. Cadence is learned, never
// dictated: no client hardcodes a TTL, and a service seen only once holds
// AUTHORIZED with no expiry stated -- no history, no verdict.
//
// Verdicts are emitted as lease.v1.LeaseVerdict on the observability topic on
// every state TRANSITION, so the bus carries the countdown the RFC's lease
// stream promised. The full table is served, not streamed, at /truth.
package lease

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/janearc/big-little-mesh/consume"
	"github.com/janearc/big-little-mesh/frood"
	leasepb "github.com/janearc/big-little-mesh/gen/go/lease/v1"
	obspb "github.com/janearc/big-little-mesh/gen/go/observability/v1"
	leaseproto "github.com/janearc/big-little-mesh/proto/lease/v1"
	obsproto "github.com/janearc/big-little-mesh/proto/observability/v1"

	"github.com/janearc/hall-monitor/pkg/metrics"
)

// Publisher is the emit seam, satisfied by emit.Publisher and faked in tests.
type Publisher interface {
	Publish(ctx context.Context, topic, subject, schemaText, key string, msg proto.Message) error
}

// graceCadences is how many of a service's own cadences may elapse before
// authorization expires. Three, matching the absence ledger's rule -- one rule
// for silence, applied to topics and to citizens alike.
const graceCadences = 3

// expiringMargin is where EXPIRING begins, in cadences. NOT 1.0, and the
// reason cost a live flap: the EWMA learns a service's cadence essentially
// EXACTLY (hm's read 14999ms against a 15s interval), so a 1.0 threshold
// marks every second between beats past the cadence as EXPIRING and flips
// back on the beat -- 50 verdicts where 4 belonged, and the operator watching
// the table saw her judge expiring on schedule. Half a cadence of margin
// absorbs jitter; genuinely missed beats still surface well before EXPIRED.
const expiringMargin = 1.5

// svc is one service's lease bookkeeping.
type svc struct {
	last    time.Time
	cadence time.Duration // EWMA of observed heartbeat gaps; zero until two beats
	state   leasepb.LeaseState
	beats   int64 // heartbeats observed this session -- the operator asked for the count
}

// SchemaResolver is the slice of consume.Resolver the authority needs -- an
// interface so tests fake the registry without HTTP.
type SchemaResolver interface {
	Subject(ctx context.Context, id int32) (string, error)
}

// resolverFallback counts ticks of the legacy residue discriminator running
// because the registry did not answer. Registered at zero in New, per fleet
// policy: the failure mode is visible BEFORE the first failure.
const resolverFallback = "hm_lease_resolver_fallback_total"

// Authority judges heartbeats into lease state. One per hm.
type Authority struct {
	ctx context.Context
	pub Publisher
	res SchemaResolver
	log *slog.Logger

	mu       sync.RWMutex
	services map[string]*svc
}

// New returns an authority. ctx scopes verdict emission; a nil pub means
// judge-but-do-not-emit, which keeps the table alive while the wire is being
// repaired rather than coupling visibility to emission. A nil res means no
// registry: discrimination falls back to unknown-field residue permanently,
// which is exactly the pre-registry behavior.
func New(ctx context.Context, pub Publisher, res SchemaResolver, log *slog.Logger) *Authority {
	metrics.Add(resolverFallback, 0)
	return &Authority{ctx: ctx, pub: pub, res: res, log: log, services: map[string]*svc{}}
}

// Observe is wired to the watcher's record hook. It ignores every topic but
// the observability topic, parses the schema-registry frame, and treats a
// record the REGISTRY says is a heartbeat as a renewal.
//
// Discrimination, primary path: the frame's schema id resolves to a subject,
// and under RecordNameStrategy the subject IS the message name -- so "is this
// a heartbeat" is the registry's answer, not an inference. Unknown fields are
// then tolerated on purpose: with the subject established, residue is schema
// evolution (a field this build has not vendored), and leases must not stall
// on it. That was the exact failure the old discriminator carried.
//
// Fallback path, registry silent: the legacy unknown-field residue check --
// LeaseVerdict rides the SAME topic, proto3 preserves wire-type mismatches as
// UNKNOWN rather than erroring (the first draft claimed otherwise; the test
// caught it), so a record with residue is refused as not-a-pure-heartbeat.
// Conservative, held for months, and a registry outage must not stall
// leases: silence degrades to the old behavior, never to a stall. The
// fallback counter exists from zero so the degraded mode is visible.
func (a *Authority) Observe(topic string, at time.Time, value []byte) {
	if topic != frood.TopicObservability {
		return
	}
	id, payload, err := consume.ParseFrame(value)
	if err != nil {
		return // not schema-registry framed; not a heartbeat
	}
	if a.res != nil {
		subject, rerr := a.res.Subject(a.ctx, id)
		if rerr == nil {
			if subject != obsproto.SubjectServiceHealthHeartbeat {
				return // the registry names it; it is not a heartbeat
			}
			var hb obspb.ServiceHealthHeartbeat
			if proto.Unmarshal(payload, &hb) != nil {
				return // registry says heartbeat, bytes disagree: refuse
			}
			a.renew(hb.GetServiceName(), at)
			return
		}
		metrics.Inc(resolverFallback)
	}

	var hb obspb.ServiceHealthHeartbeat
	if err := proto.Unmarshal(payload, &hb); err != nil {
		return // malformed payload; not a renewal
	}
	if len(hb.ProtoReflect().GetUnknown()) > 0 {
		return // decodes, but not as a pure heartbeat -- a verdict or a future subject
	}
	a.renew(hb.GetServiceName(), at)
}

// renew is one heartbeat landing: cadence learning, state transition, and
// the verdict emit -- the renewal body Observe dispatches into once the
// record is established to BE a heartbeat.
func (a *Authority) renew(name string, at time.Time) {
	if name == "" {
		return
	}

	a.mu.Lock()
	s, ok := a.services[name]
	if !ok {
		s = &svc{}
		a.services[name] = s
	}
	if !s.last.IsZero() {
		gap := at.Sub(s.last)
		if gap > 0 {
			switch {
			case s.cadence == 0:
				s.cadence = gap
			case gap > s.cadence:
				// GROW FAST: a longer gap is adopted whole. Judging a service
				// against a cadence shorter than the one it just demonstrated
				// only manufactures false EXPIRING.
				s.cadence = gap
			default:
				// SHRINK SLOW: short gaps arrive in bursts during rollouts,
				// when two pods beat under one service name and interleave --
				// an EWMA that trusted them halved the learned cadence and
				// flapped the table until it re-learned. 70/30 forgets them
				// over many beats instead.
				s.cadence = time.Duration(0.7*float64(s.cadence) + 0.3*float64(gap))
			}
		}
	}
	s.last = at
	s.beats++
	transitioned := s.state != leasepb.LeaseState_LEASE_STATE_AUTHORIZED
	s.state = leasepb.LeaseState_LEASE_STATE_AUTHORIZED
	verdict := a.verdictLocked(name, s)
	a.mu.Unlock()

	metrics.Set("hm_lease_state{service=\""+name+"\"}", int64(leasepb.LeaseState_LEASE_STATE_AUTHORIZED))
	if transitioned {
		a.emit(verdict)
	}
}

// Tick judges every known service against now. Wire it to a ticker faster
// than the shortest plausible cadence; judging is cheap and idempotent.
func (a *Authority) Tick(now time.Time) {
	type change struct{ v *leasepb.LeaseVerdict }
	var changes []change

	a.mu.Lock()
	for name, s := range a.services {
		if s.cadence == 0 {
			continue // one beat: present, but no history, so no expiry verdict
		}
		gap := now.Sub(s.last)
		var want leasepb.LeaseState
		switch {
		case gap > time.Duration(graceCadences)*s.cadence:
			want = leasepb.LeaseState_LEASE_STATE_EXPIRED
		case float64(gap) > expiringMargin*float64(s.cadence):
			want = leasepb.LeaseState_LEASE_STATE_EXPIRING
		default:
			want = leasepb.LeaseState_LEASE_STATE_AUTHORIZED
		}
		if want != s.state {
			s.state = want
			metrics.Set("hm_lease_state{service=\""+name+"\"}", int64(want))
			changes = append(changes, change{a.verdictLocked(name, s)})
		}
	}
	a.mu.Unlock()

	for _, c := range changes {
		a.emit(c.v)
	}
}

// verdictLocked builds the verdict for one service. Caller holds mu.
func (a *Authority) verdictLocked(name string, s *svc) *leasepb.LeaseVerdict {
	v := &leasepb.LeaseVerdict{
		ServiceName:   name,
		State:         s.state,
		LastHeartbeat: timestamppb.New(s.last),
		IssuedAt:      timestamppb.New(time.Now()),
		Authority:     "hm",
	}
	if s.cadence > 0 && s.state != leasepb.LeaseState_LEASE_STATE_EXPIRED {
		v.ExpiresAt = timestamppb.New(s.last.Add(time.Duration(graceCadences) * s.cadence))
	}
	return v
}

// emit publishes one verdict. Failure is counted and logged, never fatal:
// the table stays true even when the wire will not carry the announcement --
// and the counter exists from zero so the failure mode is visible BEFORE the
// first failure, which is the lesson hm's own heartbeat outage taught.
func (a *Authority) emit(v *leasepb.LeaseVerdict) {
	if a.pub == nil {
		return
	}
	if err := a.pub.Publish(a.ctx, frood.TopicObservability, leaseproto.SubjectLeaseVerdict,
		leaseproto.Schema, v.GetServiceName(), v); err != nil {
		metrics.Inc("hm_lease_emit_failed_total")
		a.log.Error("lease verdict emit failed", "service", v.GetServiceName(), "err", err)
		return
	}
	metrics.Inc("hm_lease_verdicts_total")
}

// Row is one line of the authorized table, shaped for /truth. ExpiresAt is a
// pointer because a zero time.Time defeats omitempty and renders as year one
// in the very table meant to be read at a glance -- nil means "no expiry
// stated", which is the truthful rendering of a cadence not yet learned.
type Row struct {
	Service       string     `json:"service"`
	State         string     `json:"state"`
	Beats         int64      `json:"beats"`
	LastHeartbeat time.Time  `json:"last_heartbeat"`
	ExpiresAt     *time.Time `json:"expires_at,omitempty"`
	CadenceMS     int64      `json:"cadence_ms,omitempty"`
}

// Snapshot returns the authorized table, sorted by service so the at-a-glance
// view is stable between refreshes.
func (a *Authority) Snapshot() []Row {
	a.mu.RLock()
	defer a.mu.RUnlock()
	rows := make([]Row, 0, len(a.services))
	for name, s := range a.services {
		r := Row{
			Service:       name,
			State:         stateWord(s.state),
			Beats:         s.beats,
			LastHeartbeat: s.last,
			CadenceMS:     s.cadence.Milliseconds(),
		}
		if s.cadence > 0 && s.state != leasepb.LeaseState_LEASE_STATE_EXPIRED {
			exp := s.last.Add(time.Duration(graceCadences) * s.cadence)
			r.ExpiresAt = &exp
		}
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Service < rows[j].Service })
	return rows
}

// SnapshotVerdicts returns the authorized table as lease.v1.LeaseVerdict,
// sorted by service -- the same verdicts the bus carries, built by the same
// verdictLocked, so /truth/v1 quotes the authority rather than paraphrasing
// it. One derivation, two surfaces.
func (a *Authority) SnapshotVerdicts() []*leasepb.LeaseVerdict {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]*leasepb.LeaseVerdict, 0, len(a.services))
	for name, s := range a.services {
		out = append(out, a.verdictLocked(name, s))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetServiceName() < out[j].GetServiceName() })
	return out
}

// stateWord renders the enum for humans; the glance is the requirement.
func stateWord(s leasepb.LeaseState) string {
	switch s {
	case leasepb.LeaseState_LEASE_STATE_AUTHORIZED:
		return "authorized"
	case leasepb.LeaseState_LEASE_STATE_EXPIRING:
		return "expiring"
	case leasepb.LeaseState_LEASE_STATE_EXPIRED:
		return "expired"
	default:
		return "unknown"
	}
}
