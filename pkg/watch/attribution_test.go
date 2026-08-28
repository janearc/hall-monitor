package watch

import (
	"log/slog"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func attributionWatcher() *Watcher {
	return &Watcher{
		log:           slog.Default(),
		producersSeen: map[string]time.Time{},
		groupTopics:   map[string][]string{},
	}
}

func refused(topic, key string) *kgo.Record {
	r := &kgo.Record{Topic: topic, Value: []byte("not a frame")}
	if key != "" {
		r.Key = []byte(key)
	}
	return r
}

// TestOffContractIsAttributedByRecordKey: the lease gate can only act on an
// offense it can attribute to a citizen, and the record key is what attributes
// it. Before this the key was in hand on every record and thrown away, so
// refused traffic was a property of a topic and of nobody in particular.
func TestOffContractIsAttributedByRecordKey(t *testing.T) {
	w := attributionWatcher()

	w.observe(refused("rogue.topic", "flipr"))
	w.observe(refused("rogue.topic", "flipr"))
	w.observe(refused("other.topic", "flipr"))

	at, ok := w.LastOffContract("flipr")
	if !ok {
		t.Fatal("a citizen that signed refused records must carry standing")
	}
	if at.IsZero() {
		t.Fatal("the offense time must be recorded, not merely the fact")
	}

	// standing is per citizen, not per topic: flipr answers for both topics
	counts, _, _ := w.OffContract()
	if counts["rogue.topic"] != 2 || counts["other.topic"] != 1 {
		t.Fatalf("topic counts must survive attribution: %+v", counts)
	}
}

// TestInnocentCitizenHasNoStanding: a service that signed nothing must read
// clean. This is the direction that costs a lease if it is wrong.
func TestInnocentCitizenHasNoStanding(t *testing.T) {
	w := attributionWatcher()
	w.observe(refused("rogue.topic", "flipr"))

	if _, ok := w.LastOffContract("delightd"); ok {
		t.Fatal("a citizen that signed nothing must carry no standing")
	}
}

// TestUnsignedRefusedRecordNamesNobody is the safety property the whole seam
// rests on: an unattributable offense must reach NO citizen's standing, and in
// particular the empty key must not become a citizen literally named "".
// Misattributing a lease death to an innocent service is worse than missing the
// offense entirely.
func TestUnsignedRefusedRecordNamesNobody(t *testing.T) {
	w := attributionWatcher()

	w.observe(refused("rogue.topic", ""))
	w.observe(refused("rogue.topic", ""))

	if _, ok := w.LastOffContract(""); ok {
		t.Fatal(`the empty key must not become a citizen named ""`)
	}

	counts, _, anon := w.OffContract()
	if counts["rogue.topic"] != 2 {
		t.Fatalf("unsigned refusals still count against the topic: %+v", counts)
	}
	if anon["rogue.topic"] != 2 {
		t.Fatalf("unsigned refusals must be counted as unattributable: %+v", anon)
	}
}

// TestAnonCountIsASubsetOfTheTopicCount pins the documented relationship: the
// anonymous count is part of the topic total, never a parallel population.
func TestAnonCountIsASubsetOfTheTopicCount(t *testing.T) {
	w := attributionWatcher()

	w.observe(refused("rogue.topic", "flipr"))
	w.observe(refused("rogue.topic", ""))
	w.observe(refused("rogue.topic", ""))

	counts, _, anon := w.OffContract()
	if counts["rogue.topic"] != 3 {
		t.Fatalf("want 3 refused records, got %d", counts["rogue.topic"])
	}
	if anon["rogue.topic"] != 2 {
		t.Fatalf("want 2 of them unattributable, got %d", anon["rogue.topic"])
	}
	if anon["rogue.topic"] > counts["rogue.topic"] {
		t.Fatal("the anonymous count cannot exceed the topic count")
	}
}

// TestWellFramedTrafficCreatesNoStanding: only REFUSED records are offenses. A
// citizen producing correctly framed records must never acquire standing from
// them, however many it sends.
func TestWellFramedTrafficCreatesNoStanding(t *testing.T) {
	w := attributionWatcher()

	framed := &kgo.Record{
		Topic: "delight.events",
		Key:   []byte("delightd"),
		Value: []byte{0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x01},
	}
	w.observe(framed)
	w.observe(framed)

	if _, ok := w.LastOffContract("delightd"); ok {
		t.Fatal("contract-abiding traffic must not create standing")
	}
	counts, _, _ := w.OffContract()
	if len(counts) != 0 {
		t.Fatalf("no topic should carry refusals: %+v", counts)
	}
}

// TestOffContractCopiesAreNotLiveReferences extends the existing copy check to
// the anonymous map, which is new and shares the same lock.
func TestOffContractCopiesAreNotLiveReferences(t *testing.T) {
	w := attributionWatcher()
	w.observe(refused("rogue.topic", ""))

	_, _, anon := w.OffContract()
	anon["rogue.topic"] = 999

	_, _, fresh := w.OffContract()
	if fresh["rogue.topic"] != 1 {
		t.Fatal("OffContract returned a live reference to the anonymous map")
	}
}
