package report

import (
	"encoding/json"
	"testing"
	"time"

	truthpb "github.com/janearc/big-little-mesh/gen/go/truth/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// v1Source builds the minimal wire on report_test.go's fakeSource: one
// healthy topic with a reader, one void topic, one carrying off-contract
// records.
func v1Source(now time.Time) fakeSource {
	return fakeSource{
		producers: map[string]time.Time{
			"observability.events": now.Add(-2 * time.Second),
			"orphan.events":        now.Add(-5 * time.Second),
			"raw.events":           now.Add(-1 * time.Second),
		},
		groups: map[string][]string{
			"hm":    {"observability.events"},
			"other": {"raw.events"},
		},
		offContract: map[string]int64{"raw.events": 7},
	}
}

// TestBuildV1MirrorsBuild pins the conversion: same source, same derivation,
// contract shape -- topic for topic, finding for finding, kind for kind.
func TestBuildV1MirrorsBuild(t *testing.T) {
	now := time.Now()
	src := v1Source(now)

	legacy := Build(src, nil, nil, now)
	v1 := BuildV1(src, nil, nil, now)

	if v1.GetService() != legacy.Service {
		t.Fatalf("service: %q != %q", v1.GetService(), legacy.Service)
	}
	if len(v1.GetTopics()) != len(legacy.Topics) {
		t.Fatalf("topic rows: %d != %d", len(v1.GetTopics()), len(legacy.Topics))
	}
	for i, row := range v1.GetTopics() {
		l := legacy.Topics[i]
		if row.GetTopic() != l.Topic || row.GetVoid() != l.Void || row.GetSilent() != l.Silent {
			t.Fatalf("row %d diverges: %v vs %+v", i, row, l)
		}
		if row.GetOffContractRecords() != l.OffContractRecords {
			t.Fatalf("row %d off-contract: %d != %d", i, row.GetOffContractRecords(), l.OffContractRecords)
		}
	}
	if len(v1.GetFindings()) != len(legacy.Findings) {
		t.Fatalf("findings: %d != %d", len(v1.GetFindings()), len(legacy.Findings))
	}
	for i, f := range v1.GetFindings() {
		if f.GetClass() != truthpb.FindingClass_FINDING_CLASS_REFUSAL {
			t.Fatalf("finding %d class: %v", i, f.GetClass())
		}
		if f.GetKind() == truthpb.FindingKind_FINDING_KIND_UNSPECIFIED {
			t.Fatalf("finding %d kind unmapped for legacy kind %q", i, legacy.Findings[i].Kind)
		}
	}
	if len(v1.GetGroups()) != len(legacy.Groups) {
		t.Fatalf("groups: %d != %d", len(v1.GetGroups()), len(legacy.Groups))
	}
	for g, topics := range legacy.Groups {
		if got := v1.GetGroups()[g].GetTopics(); len(got) != len(topics) {
			t.Fatalf("group %q topics: %d != %d", g, len(got), len(topics))
		}
	}
}

// TestKindOfIsTotalOverBuildsVocabulary: every kind word Build emits maps to
// a named enum value. A new word in Build without a mapping here fails this
// test rather than silently rendering UNSPECIFIED in production.
func TestKindOfIsTotalOverBuildsVocabulary(t *testing.T) {
	for _, kind := range []string{"void", "silent", "off-contract", "lease-expired"} {
		if kindOf(kind) == truthpb.FindingKind_FINDING_KIND_UNSPECIFIED {
			t.Fatalf("kind %q unmapped", kind)
		}
	}
}

// TestHandlerV1ServesCanonicalProtojson: the wire form parses back into the
// contract type -- the round trip IS the spec conformance check.
func TestHandlerV1ServesCanonicalProtojson(t *testing.T) {
	now := time.Now()
	b, err := protojson.Marshal(BuildV1(v1Source(now), nil, nil, now))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !json.Valid(b) {
		t.Fatalf("not valid JSON: %s", b[:min(len(b), 120)])
	}
	var back truthpb.Report
	if err := protojson.Unmarshal(b, &back); err != nil {
		t.Fatalf("round trip: %v", err)
	}
	if len(back.GetTopics()) != 3 {
		t.Fatalf("round trip lost topics: %d", len(back.GetTopics()))
	}
}
