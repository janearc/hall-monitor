// Package watch is hm's observation surface: the consume-everything loop and
// the broker introspection tick. It is the passive half of the RFC —
// read-only against production, resident, mechanical, no LLM. It observes
// two things the fleet cannot see about itself: what is actually on the wire
// (every record on every topic), and who is actually listening (consumer
// groups, their subscriptions, their lag).
package watch

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/janearc/hall-monitor/pkg/metrics"
)

// isInternalTopic reports whether the broker or registry owns the topic
// (leading underscore: _schemas, __consumer_offsets). A byte check, not a
// pattern — there is exactly one rule and it lives at index 0.
func isInternalTopic(topic string) bool {
	return len(topic) > 0 && topic[0] == '_'
}

// Observation is one record's identity on the wire: where it appeared and
// what schema it claimed. The ledger (next change) accumulates these; today
// they feed counters and the in-memory state the truth report will read.
type Observation struct {
	Topic    string
	SchemaID int32 // -1 when the record is not a Confluent SR frame
	At       time.Time
}

// Watcher owns the consuming client and the introspection tick.
type Watcher struct {
	client *kgo.Client
	adm    *kadm.Client
	log    *slog.Logger

	mu sync.RWMutex
	// consuming is the explicit set of topics the client has been told to
	// consume. Explicit, not a pattern: the introspection tick discovers new
	// topics from the broker and adds them by name, so the consumed set is
	// always auditable and never depends on pattern semantics.
	consuming map[string]bool
	// producersSeen is topic -> last time hm saw a record on it. This is the
	// seed of the absence ledger: presence with history.
	producersSeen map[string]time.Time
	// groupTopics is consumer group -> topics it consumes, from the last
	// introspection tick. Zero groups on a producing topic = the void.
	groupTopics map[string][]string

	// onRecord, when set, is called for every observed record (any framing).
	// The absence ledger rides this seam. Set before Run; not synchronized.
	onRecord func(topic string, at time.Time)
}

// OnRecord registers a per-record callback. MUST be called before Run.
func (w *Watcher) OnRecord(fn func(topic string, at time.Time)) {
	w.onRecord = fn
}

// New connects the watcher and subscribes it to every current non-internal
// topic, by name. Unlike other froods, hm does not treat a down broker as
// best-effort: the caller marks /health degraded — an hm that cannot see
// never pretends otherwise.
func New(ctx context.Context, brokers []string, log *slog.Logger) (*Watcher, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no kafka brokers configured")
	}
	if log == nil {
		log = slog.Default()
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("hm"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		return nil, err
	}
	w := &Watcher{
		client:        cl,
		adm:           kadm.NewClient(cl),
		log:           log,
		consuming:     map[string]bool{},
		producersSeen: map[string]time.Time{},
		groupTopics:   map[string][]string{},
	}
	// subscribe to the current topic set before returning, so a watcher that
	// cannot even list topics fails construction loudly instead of running blind
	if err := w.addNewTopics(ctx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("initial topic subscription: %w", err)
	}
	return w, nil
}

// Close leaves the consumer group explicitly, then releases the client. The
// explicit leave matters: a member that vanishes without leaving makes the
// broker wait out the session timeout before rebalancing, stalling every
// other consumer in the group — hm goes out of its way to never be the
// cause of the class of outage it exists to catch.
func (w *Watcher) Close(ctx context.Context) {
	if w == nil || w.client == nil {
		return
	}
	if err := w.client.LeaveGroupContext(ctx); err != nil {
		w.log.Warn("group leave on shutdown failed (broker will time the member out)", "err", err)
	}
	w.client.Close()
}

// addNewTopics lists the broker's topics and subscribes, by name, to any
// non-internal topic not already consumed.
func (w *Watcher) addNewTopics(ctx context.Context) error {
	details, err := w.adm.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("list topics: %w", err)
	}
	var fresh []string
	w.mu.Lock()
	for topic := range details {
		if isInternalTopic(topic) || w.consuming[topic] {
			continue
		}
		w.consuming[topic] = true
		fresh = append(fresh, topic)
	}
	w.mu.Unlock()
	if len(fresh) > 0 {
		sort.Strings(fresh)
		w.client.AddConsumeTopics(fresh...)
		w.log.Info("subscribed to topics", "added", fresh)
		metrics.Add("hm_topics_subscribed_total", int64(len(fresh)))
	}
	return nil
}

// Run drives both loops until ctx cancels: the poll loop inline, the
// introspection tick in its own goroutine. Both are hm's hot paths and are
// instrumented as such (hm_watch_*, hm_introspect_*) — if hm is the
// problem, hm says so.
func (w *Watcher) Run(ctx context.Context, tick time.Duration) {
	if w == nil {
		return
	}
	go w.introspectLoop(ctx, tick)
	for {
		// cancellation is checked at the top of every iteration so shutdown
		// never waits on a fetch
		select {
		case <-ctx.Done():
			return
		default:
		}
		// PollFetches blocks until records arrive, an error surfaces, or ctx
		// cancels. Errors arrive per topic-partition: log and count each,
		// then poll again — kgo owns retry and backoff internally, so this
		// loop's only job on error is to record that it happened and keep
		// observing everything else.
		fetches := w.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			if ctx.Err() != nil {
				return
			}
			for _, e := range errs {
				metrics.Inc("hm_watch_poll_errors_total")
				w.log.Error("watch poll error", "topic", e.Topic, "err", e.Err)
			}
			continue
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			w.observe(rec)
		})
	}
}

// observe classifies one record: SR-framed traffic is counted under its
// schema id; anything else on a fleet topic is off-contract — refusal-class
// per the RFC table, surfaced as an ERROR and a counter today (the ledger
// change turns it into a durable finding).
func (w *Watcher) observe(rec *kgo.Record) {
	obs := Observation{Topic: rec.Topic, SchemaID: -1, At: time.Now()}
	if id, err := frameSchemaID(rec.Value); err == nil {
		obs.SchemaID = id
		metrics.Inc(fmt.Sprintf("hm_records_total{topic=%q,schema_id=\"%d\"}", rec.Topic, id))
	} else {
		metrics.Inc(fmt.Sprintf("hm_off_contract_total{topic=%q}", rec.Topic))
		w.log.Error("off-contract traffic (refusal-class)", "topic", rec.Topic, "err", err)
	}
	w.mu.Lock()
	w.producersSeen[rec.Topic] = obs.At
	w.mu.Unlock()
	if w.onRecord != nil {
		w.onRecord(rec.Topic, obs.At)
	}
}

// introspectLoop asks the broker who exists and who listens, on a jittered
// tick so hm cannot self-synchronize into a periodic spike.
func (w *Watcher) introspectLoop(ctx context.Context, tick time.Duration) {
	if tick < 5*time.Second {
		tick = time.Minute
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(jitter(tick)):
		}
		start := time.Now()
		if err := w.introspect(ctx); err != nil {
			metrics.Inc("hm_introspect_errors_total")
			w.log.Error("introspection tick failed", "err", err)
		}
		metrics.Add("hm_introspect_duration_ms_total", time.Since(start).Milliseconds())
		metrics.Inc("hm_introspect_ticks_total")
	}
}

// jitter returns a duration uniform in [0.8*tick, 1.2*tick), integer math
// throughout — the standard smear for periodic workers (same family as full
// jitter, bounded tighter because this tick also drives topic discovery and
// should not wander): uniform noise around the base breaks alignment
// between periodic loops so their load cannot stack into synchronized
// spikes.
func jitter(tick time.Duration) time.Duration {
	span := tick / 5 * 2 // the 40% window, as a Duration (int64 ns)
	return tick - tick/5 + time.Duration(rand.Int64N(int64(span)))
}

// introspect refreshes the topic subscription and the who-listens map, then
// flags the void: topics with live producers and zero live consumer groups.
// hm's own group is excluded — hm listening does not make a topic heard.
func (w *Watcher) introspect(ctx context.Context) error {
	// new topics first: discovery drives the explicit subscription (never a
	// pattern), so a topic born between ticks is observed within one tick
	if err := w.addNewTopics(ctx); err != nil {
		return err
	}
	groups, err := w.adm.DescribeGroups(ctx)
	if err != nil {
		return fmt.Errorf("describe groups: %w", err)
	}
	listening := map[string][]string{} // group -> topics
	hasConsumer := map[string]bool{}
	for _, g := range groups {
		if g.Group == "hm" {
			continue
		}
		for _, m := range g.Members {
			c, ok := m.Assigned.AsConsumer()
			if !ok {
				// a member whose assignment is not consumer-shaped (custom
				// group protocols, kafka-connect style). None exist in this
				// fleet, so seeing one IS a finding: count it, name it, keep
				// going — it is metadata hm cannot yet read, not a reason to
				// fail the whole tick.
				metrics.Inc("hm_nonconsumer_group_members_total")
				w.log.Warn("group member with non-consumer assignment (unreadable metadata)",
					"group", g.Group)
				continue
			}
			for _, t := range c.Topics {
				listening[g.Group] = append(listening[g.Group], t.Topic)
				hasConsumer[t.Topic] = true
			}
		}
	}

	w.mu.Lock()
	w.groupTopics = listening
	seen := make(map[string]time.Time, len(w.producersSeen))
	for t, at := range w.producersSeen {
		seen[t] = at
	}
	w.mu.Unlock()

	voids := findVoids(seen, hasConsumer)
	for _, topic := range voids {
		metrics.Inc(fmt.Sprintf("hm_void_topics_total{topic=%q}", topic))
		w.log.Error("producing with zero live consumer groups (refusal-class)",
			"topic", topic, "last_record", seen[topic].UTC().Format(time.RFC3339))
	}
	w.log.Info("introspection tick",
		"groups", len(listening), "producing_topics", len(seen), "void_topics", len(voids))
	return nil
}

// findVoids returns the topics that have a live producer (seen) and no live
// consumer group (hasConsumer), excluding broker internals. Pure so the void
// rule — the exact failure class of the silent week — is testable without a
// broker.
func findVoids(seen map[string]time.Time, hasConsumer map[string]bool) []string {
	var voids []string
	for topic := range seen {
		if isInternalTopic(topic) || hasConsumer[topic] {
			continue
		}
		voids = append(voids, topic)
	}
	sort.Strings(voids)
	return voids
}

// Snapshot returns the current who-talks/who-listens state for the truth
// report (next change). Copies, so callers cannot race the watcher.
func (w *Watcher) Snapshot() (producers map[string]time.Time, groups map[string][]string) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	producers = make(map[string]time.Time, len(w.producersSeen))
	for t, at := range w.producersSeen {
		producers[t] = at
	}
	groups = make(map[string][]string, len(w.groupTopics))
	for g, ts := range w.groupTopics {
		groups[g] = append([]string(nil), ts...)
	}
	return producers, groups
}

// frameSchemaID reads the Confluent SR wire header and returns the schema
// id. hm keeps the id — it is the record's claimed identity, the thing hm
// exists to check — where the fleet's consumers discard it (their type is
// fixed by topic). The error path is not a not-my-job shrug; it is the
// detection this service was built for: unframed bytes on a fleet topic
// mean a producer operating outside the contract system — a console
// producer, a misconfigured client, or hand-rolled protocol code. observe()
// counts it and logs it refusal-class, and the record still advances (hm is
// passive in v0; under leases this exact signal is what stops an emitter's
// traffic).
func frameSchemaID(frame []byte) (int32, error) {
	if len(frame) < 5 || frame[0] != 0x00 {
		return -1, fmt.Errorf("no SR framing (len=%d): producer outside the contract system", len(frame))
	}
	return int32(binary.BigEndian.Uint32(frame[1:5])), nil
}
