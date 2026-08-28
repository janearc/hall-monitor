// The truth.v1 surface: the same report, spoken as the contract. Build stays
// the single derivation of truth; this file only converts its output, plus
// the authority's own verdicts, into truth.v1.Report -- mapping is mechanical
// on purpose, so the two surfaces cannot drift in logic, only in shape, and
// the shape is pinned by the contract.
package report

import (
	"net/http"
	"time"

	truthpb "github.com/janearc/big-little-mesh/gen/go/truth/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/janearc/hall-monitor/pkg/lease"
	"github.com/janearc/hall-monitor/pkg/ledger"
)

// kindOf maps Build's finding-kind words to the contract enum. The words are
// a closed set defined in Build; an unknown word maps to UNSPECIFIED rather
// than guessing, and that is a defect to notice, not smooth over.
func kindOf(kind string) truthpb.FindingKind {
	switch kind {
	case "void":
		return truthpb.FindingKind_FINDING_KIND_VOID
	case "silent":
		return truthpb.FindingKind_FINDING_KIND_SILENT
	case "off-contract":
		return truthpb.FindingKind_FINDING_KIND_OFF_CONTRACT
	case "lease-expired":
		return truthpb.FindingKind_FINDING_KIND_LEASE_EXPIRED
	case "renewal-refused":
		return truthpb.FindingKind_FINDING_KIND_RENEWAL_REFUSED
	default:
		return truthpb.FindingKind_FINDING_KIND_UNSPECIFIED
	}
}

// BuildV1 assembles the truth.v1 report: Build's derivation for topics,
// groups and findings; the authority's own verdicts, verbatim, for the
// authorized table.
func BuildV1(src Source, led *ledger.Ledger, auth *lease.Authority, now time.Time) *truthpb.Report {
	r := Build(src, led, auth, now)

	out := &truthpb.Report{
		Service:     r.Service,
		GeneratedAt: timestamppb.New(r.GeneratedAt),
		Groups:      make(map[string]*truthpb.TopicList, len(r.Groups)),
	}
	if auth != nil {
		out.Authorized = auth.SnapshotVerdicts()
	}
	for g, topics := range r.Groups {
		out.Groups[g] = &truthpb.TopicList{Topics: topics}
	}
	for _, t := range r.Topics {
		row := &truthpb.TopicRow{
			Topic:              t.Topic,
			LastRecord:         timestamppb.New(t.LastRecord),
			Consumers:          t.Consumers,
			Void:               t.Void,
			Silent:             t.Silent,
			OffContractRecords: t.OffContractRecords,
		}
		if t.Silent {
			row.ExpectedCadence = durationpb.New(time.Duration(t.ExpectedCadenceMS) * time.Millisecond)
			row.SilentFor = durationpb.New(time.Duration(t.SilentForMS) * time.Millisecond)
		}
		if t.LastOffContract != nil {
			row.LastOffContract = timestamppb.New(*t.LastOffContract)
		}
		out.Topics = append(out.Topics, row)
	}
	for _, f := range r.Findings {
		out.Findings = append(out.Findings, &truthpb.Finding{
			Class:   truthpb.FindingClass_FINDING_CLASS_REFUSAL,
			Kind:    kindOf(f.Kind),
			Topic:   f.Topic,
			Service: f.Service,
			Detail:  f.Detail,
		})
	}
	return out
}

// HandlerV1 serves /truth/v1: canonical protojson of truth.v1.Report, at
// request time, never cached -- the report is a statement about now.
func HandlerV1(src Source, led *ledger.Ledger, auth *lease.Authority) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		b, err := protojson.Marshal(BuildV1(src, led, auth, time.Now()))
		if err != nil {
			http.Error(w, `{"error":"marshal failed"}`, http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(b)
	})
}
