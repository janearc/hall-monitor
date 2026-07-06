package watch

import (
	"log/slog"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestFrameSchemaID(t *testing.T) {
	// a well-formed frame: magic 0x00, schema id 7, index 0x00, payload
	id, err := frameSchemaID([]byte{0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0xde, 0xad})
	if err != nil {
		t.Fatalf("well-formed frame rejected: %v", err)
	}
	if id != 7 {
		t.Fatalf("schema id = %d, want 7", id)
	}

	// off-contract shapes: no magic, and truncated
	if _, err := frameSchemaID([]byte(`{"json":"no"}`)); err == nil {
		t.Fatal("bare JSON accepted as a frame")
	}
	if _, err := frameSchemaID([]byte{0x00, 0x00}); err == nil {
		t.Fatal("truncated frame accepted")
	}
}

func TestObserveClassifies(t *testing.T) {
	w := &Watcher{
		log:           slog.Default(),
		producersSeen: map[string]time.Time{},
		groupTopics:   map[string][]string{},
	}
	// SR-framed record: counted and remembered as a producer
	w.observe(&kgo.Record{Topic: "delight.events", Value: []byte{0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x01}})
	// off-contract record: still remembered (it IS traffic), flagged separately
	w.observe(&kgo.Record{Topic: "rogue.topic", Value: []byte("not a frame")})

	if _, ok := w.producersSeen["delight.events"]; !ok {
		t.Fatal("framed record not recorded as producer")
	}
	if _, ok := w.producersSeen["rogue.topic"]; !ok {
		t.Fatal("off-contract record not recorded as producer")
	}
}

func TestIsInternalTopic(t *testing.T) {
	for topic, want := range map[string]bool{
		"_schemas": true, "__consumer_offsets": true, "delight.events": false, "": false,
	} {
		if got := isInternalTopic(topic); got != want {
			t.Fatalf("isInternalTopic(%q) = %v, want %v", topic, got, want)
		}
	}
}

func TestJitterBounds(t *testing.T) {
	tick := time.Minute
	for range 1000 {
		d := jitter(tick)
		if d < 48*time.Second || d >= 72*time.Second {
			t.Fatalf("jitter(%v) = %v, outside [0.8t, 1.2t)", tick, d)
		}
	}
}

func TestFindVoids(t *testing.T) {
	now := time.Now()
	seen := map[string]time.Time{
		"observability.transfers": now, // taco's void, the week we lived
		"delight.events":          now,
		"_schemas":                now, // broker internal: never a void
	}
	hasConsumer := map[string]bool{"delight.events": true}
	voids := findVoids(seen, hasConsumer)
	if len(voids) != 1 || voids[0] != "observability.transfers" {
		t.Fatalf("voids = %v, want [observability.transfers]", voids)
	}
}

func TestSnapshotCopies(t *testing.T) {
	w := &Watcher{
		producersSeen: map[string]time.Time{"delight.events": time.Now()},
		groupTopics:   map[string][]string{"obs": {"delight.events"}},
	}
	producers, groups := w.Snapshot()
	// mutating the snapshot must not reach the watcher's state
	delete(producers, "delight.events")
	groups["obs"][0] = "mutated"
	if _, ok := w.producersSeen["delight.events"]; !ok {
		t.Fatal("snapshot shares the producers map")
	}
	if w.groupTopics["obs"][0] != "delight.events" {
		t.Fatal("snapshot shares the group slice")
	}
}
