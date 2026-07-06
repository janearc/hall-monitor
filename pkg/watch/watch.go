// Package watch is hm's eyes: the consume-everything loop and the broker
// introspection tick. It is the passive half of the RFC — read-only against
// production, resident, mechanical, no LLM. It observes two things the rest
// of the fleet cannot see about itself: what is actually on the wire (every
// record on every topic), and who is actually listening (consumer groups,
// their subscriptions, their lag).
package watch

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/janearc/hall-monitor/pkg/metrics"
)

// internalTopic matches topics the broker and registry own (double-underscore
// internals, _schemas); hm observes the fleet's traffic, not kafka's own.
var internalTopic = regexp.MustCompile(`^_`)

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
	// producersSeen is topic -> last time hm saw a record on it. This is the
	// seed of the absence ledger: presence with history.
	producersSeen map[string]time.Time
	// groupTopics is consumer group -> topics it consumes, from the last
	// introspection tick. Zero groups on a producing topic = the void.
	groupTopics map[string][]string
}

// New connects the watcher. Unlike other froods, hm does not treat a down
// broker as best-effort: the caller marks /health degraded and retries — a
// blind hm never pretends otherwise.
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
		kgo.ConsumeRegex(),
		// every topic that is not broker-internal; new topics are picked up
		// by the same regex as metadata refreshes, so a contract hm does not
		// know still becomes an observation (and a finding), never a blind spot
		kgo.ConsumeTopics(`^[^_].*`),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		return nil, err
	}
	if err := cl.Ping(ctx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("kafka unreachable: %w", err)
	}
	return &Watcher{
		client:        cl,
		adm:           kadm.NewClient(cl),
		log:           log,
		producersSeen: map[string]time.Time{},
		groupTopics:   map[string][]string{},
	}, nil
}

// Close releases the clients.
func (w *Watcher) Close() {
	if w != nil && w.client != nil {
		w.client.Close()
	}
}

// Run drives both loops until ctx cancels: the poll loop inline, the
// introspection tick in its own goroutine. Both are hm's hot paths and are
// instrumented as such (hm_watch_* counters) — if hm is the problem, hm says so.
func (w *Watcher) Run(ctx context.Context, tick time.Duration) {
	if w == nil {
		return
	}
	go w.introspectLoop(ctx, tick)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
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
}

// introspectLoop asks the broker who exists and who listens, on a jittered
// tick so hm cannot self-synchronize into a periodic spike.
func (w *Watcher) introspectLoop(ctx context.Context, tick time.Duration) {
	if tick <= 0 {
		tick = time.Minute
	}
	for {
		// jitter +-20% around the tick
		d := tick + time.Duration((rand.Float64()-0.5)*0.4*float64(tick))
		select {
		case <-ctx.Done():
			return
		case <-time.After(d):
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

// introspect refreshes the who-listens map and flags the void: topics with
// live producers and zero live consumer groups. hm's own group is excluded —
// hm listening does not make a topic heard.
func (w *Watcher) introspect(ctx context.Context) error {
	groups, err := w.adm.DescribeGroups(ctx)
	if err != nil {
		return fmt.Errorf("describe groups: %w", err)
	}
	listening := map[string][]string{} // group -> topics
	topicHasEar := map[string]bool{}
	for _, g := range groups {
		if g.Group == "hm" {
			continue
		}
		for _, m := range g.Members {
			c, ok := m.Assigned.AsConsumer()
			if !ok {
				continue
			}
			for _, t := range c.Topics {
				listening[g.Group] = append(listening[g.Group], t.Topic)
				topicHasEar[t.Topic] = true
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

	voids := findVoids(seen, topicHasEar)
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
// consumer group (hasEar), excluding broker internals. Pure so the void rule
// — the exact failure class of the silent week — is testable without a broker.
func findVoids(seen map[string]time.Time, hasEar map[string]bool) []string {
	var voids []string
	for topic := range seen {
		if internalTopic.MatchString(topic) || hasEar[topic] {
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

// frameSchemaID reads the Confluent SR wire header and returns the schema id.
// hm keeps the id — it is the record's claimed identity, the thing hm exists
// to check — where the fleet's consumers discard it (their type is fixed by
// topic). Anything without the magic byte is not contract traffic.
func frameSchemaID(frame []byte) (int32, error) {
	if len(frame) < 5 || frame[0] != 0x00 {
		return -1, fmt.Errorf("not a confluent SR frame (len=%d)", len(frame))
	}
	return int32(binary.BigEndian.Uint32(frame[1:5])), nil
}
