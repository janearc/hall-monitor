package lease

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/janearc/big-little-mesh/frood"
)

// fakeStanding is a Standing the test drives directly: it answers with
// whatever offense time the test last recorded for a service.
type fakeStanding struct{ last map[string]time.Time }

func (f *fakeStanding) LastOffContract(service string) (time.Time, bool) {
	at, ok := f.last[service]
	return at, ok
}

func standingAuthority() (*Authority, *fakePub, *fakeStanding) {
	pub := &fakePub{}
	std := &fakeStanding{last: map[string]time.Time{}}
	return New(context.Background(), pub, nil, std, slog.Default()), pub, std
}

// row returns the snapshot row for one service, failing if it is absent.
func row(t *testing.T, a *Authority, service string) Row {
	t.Helper()
	for _, r := range a.Snapshot() {
		if r.Service == service {
			return r
		}
	}
	t.Fatalf("no row for %q in %+v", service, a.Snapshot())
	return Row{}
}

// TestRefusedRenewalDecaysTheLeaseWhileTheCitizenIsStillBeating is the design
// doc's claim itself (rfc-hall-monitor 3.4: renewal is automatic ONLY while no
// open refusal-class finding stands). Before this gate a service emitting
// refused traffic held a healthy lease the entire time it did it, because the
// heartbeat alone renewed. Here the heartbeats never stop and the lease dies
// anyway -- that gap between "alive" and "authorized" IS the feature.
func TestRefusedRenewalDecaysTheLeaseWhileTheCitizenIsStillBeating(t *testing.T) {
	a, _, std := standingAuthority()
	start := time.Now()
	const cadence = 10 * time.Second

	// two clean beats: the cadence is learned and the lease is good
	a.Observe(frood.TopicObservability, start, beat(t, "flipr"))
	a.Observe(frood.TopicObservability, start.Add(cadence), beat(t, "flipr"))
	if r := row(t, a, "flipr"); r.State != "authorized" || r.RenewalRefused {
		t.Fatalf("clean citizen must be authorized and unrefused: %+v", r)
	}

	// the offense begins, and recurs on every beat, so it never ages out
	for i := 2; i <= 6; i++ {
		at := start.Add(time.Duration(i) * cadence)
		std.last["flipr"] = at.Add(-cadence) // refused traffic one cadence ago
		a.Observe(frood.TopicObservability, at, beat(t, "flipr"))
		a.Tick(at)
	}

	r := row(t, a, "flipr")
	if r.State != "expired" {
		t.Fatalf("a citizen that never renews must expire, got state %q (%+v)", r.State, r)
	}
	if !r.RenewalRefused {
		t.Fatalf("the row must say WHY it expired while beating: %+v", r)
	}
	// the heartbeats were observed the whole time -- refusal gates renewal, it
	// does not blind hm to the citizen's presence
	if r.Beats != 7 {
		t.Fatalf("every beat must still be observed, got %d", r.Beats)
	}
	if r.LastHeartbeat.Before(start.Add(6 * cadence)) {
		t.Fatalf("last heartbeat must be the true one, got %v", r.LastHeartbeat)
	}
}

// TestStandingSelfClearsWhenTheBehaviorStops pins the v0 close path infra-4
// ruled: standing is open only while the wire still shows the offense inside
// three of the citizen's own cadences. This is the explicit stand-in for the
// surrogate session (v2), and without it a single refused record would be a
// permanent lease death with no operator way back.
func TestStandingSelfClearsWhenTheBehaviorStops(t *testing.T) {
	a, _, std := standingAuthority()
	start := time.Now()
	const cadence = 10 * time.Second

	a.Observe(frood.TopicObservability, start, beat(t, "flipr"))
	a.Observe(frood.TopicObservability, start.Add(cadence), beat(t, "flipr"))

	// one offense, then the service behaves
	offense := start.Add(cadence)
	std.last["flipr"] = offense

	a.Observe(frood.TopicObservability, start.Add(2*cadence), beat(t, "flipr"))
	if r := row(t, a, "flipr"); !r.RenewalRefused {
		t.Fatalf("an offense one cadence old must stand: %+v", r)
	}

	// keep beating ON cadence until the offense falls outside the window. The
	// beats must stay regular: the learner GROWS FAST, so a skipped beat would
	// widen the very window under test.
	for i := 3; i <= 5; i++ {
		at := start.Add(time.Duration(i) * cadence)
		a.Observe(frood.TopicObservability, at, beat(t, "flipr"))
		a.Tick(at)
	}

	r := row(t, a, "flipr")
	if r.RenewalRefused {
		t.Fatalf("standing must clear once the offense ages out: %+v", r)
	}
	if r.State != "authorized" {
		t.Fatalf("a citizen back in good standing renews, got %q (%+v)", r.State, r)
	}
}

// TestRefusalWindowIsTheCitizensOwnCadence proves the window is measured in the
// service's OWN learned cadence rather than a fixed duration -- the same
// "against its own history" rule the absence ledger uses for silence. One
// offense age, two services, opposite verdicts.
func TestRefusalWindowIsTheCitizensOwnCadence(t *testing.T) {
	a, _, std := standingAuthority()
	start := time.Now()

	// each service beats regularly at its own rate, so each learns its own
	// cadence: one second for fast, one minute for slow
	for i := range 11 {
		a.Observe(frood.TopicObservability, start.Add(time.Duration(i)*time.Second), beat(t, "fast"))
	}
	for i := range 3 {
		a.Observe(frood.TopicObservability, start.Add(time.Duration(i)*time.Minute), beat(t, "slow"))
	}

	// ONE offense age for both: ten seconds before each one's latest beat
	fastNow := start.Add(10 * time.Second)
	slowNow := start.Add(2 * time.Minute)
	std.last["fast"] = fastNow.Add(-10 * time.Second)
	std.last["slow"] = slowNow.Add(-10 * time.Second)

	a.Observe(frood.TopicObservability, fastNow.Add(time.Second), beat(t, "fast"))
	a.Observe(frood.TopicObservability, slowNow.Add(time.Minute), beat(t, "slow"))

	// ten seconds is many cadences for fast (window three seconds) and a
	// fraction of one for slow (window three minutes), so the identical
	// offense age stands against slow and has aged out for fast
	if r := row(t, a, "fast"); r.RenewalRefused {
		t.Fatalf("an offense far outside fast's own window must not stand: %+v", r)
	}
	if r := row(t, a, "slow"); !r.RenewalRefused {
		t.Fatalf("an offense well inside slow's own window must stand: %+v", r)
	}
}

// TestNoCadenceNoRefusal: a service seen once has no window to measure an
// offense against, and hm does not call a refusal it cannot cite -- the same
// refusal-default the absence ledger applies to its own claims.
func TestNoCadenceNoRefusal(t *testing.T) {
	a, _, std := standingAuthority()
	at := time.Now()
	std.last["flipr"] = at

	a.Observe(frood.TopicObservability, at, beat(t, "flipr"))

	if r := row(t, a, "flipr"); r.RenewalRefused || r.State != "authorized" {
		t.Fatalf("one beat carries no cadence, so no refusal is citable: %+v", r)
	}
}

// TestUnattributedOffenseCostsNobodyTheLease: an offense the watcher could not
// attribute reaches no citizen's standing. Misattributing a lease death to an
// innocent service is worse than missing the offense, so the absent case must
// renew normally.
func TestUnattributedOffenseCostsNobodyTheLease(t *testing.T) {
	a, _, std := standingAuthority()
	start := time.Now()
	const cadence = 10 * time.Second

	// an offense stands against someone else entirely
	std.last["other"] = start.Add(cadence)

	a.Observe(frood.TopicObservability, start, beat(t, "flipr"))
	a.Observe(frood.TopicObservability, start.Add(cadence), beat(t, "flipr"))
	a.Observe(frood.TopicObservability, start.Add(2*cadence), beat(t, "flipr"))

	if r := row(t, a, "flipr"); r.RenewalRefused || r.State != "authorized" {
		t.Fatalf("an innocent citizen must renew: %+v", r)
	}
}

// TestNilStandingRenewsOnHeartbeatAlone pins the documented degradation: with
// no standing source the authority behaves exactly as it did before the gate
// existed, rather than refusing everyone or panicking.
func TestNilStandingRenewsOnHeartbeatAlone(t *testing.T) {
	a, _ := testAuthority() // built with a nil Standing
	start := time.Now()
	const cadence = 10 * time.Second

	for i := range 4 {
		at := start.Add(time.Duration(i) * cadence)
		a.Observe(frood.TopicObservability, at, beat(t, "flipr"))
		a.Tick(at)
	}

	if r := row(t, a, "flipr"); r.RenewalRefused || r.State != "authorized" {
		t.Fatalf("no standing source means renew on heartbeat alone: %+v", r)
	}
}
