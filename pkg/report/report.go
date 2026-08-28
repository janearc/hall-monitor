// Package report builds the truth report: the machine-readable statement of
// who is talking, to whom, and what is yelling into the void. Rows are small,
// self-contained and independently judgeable on purpose — this document is
// what the assessment tier will read.
package report

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/janearc/hall-monitor/pkg/lease"
	"github.com/janearc/hall-monitor/pkg/ledger"
)

// Source is the watcher's read seam: who produced (and when last), and which
// groups consume which topics.
type Source interface {
	Snapshot() (producers map[string]time.Time, groups map[string][]string)
	// OffContract is who is on the wire but NOT in agreement with the
	// contracts: per-topic counts of un-frameable records, and when the last
	// one arrived.
	OffContract() (counts map[string]int64, last map[string]time.Time)
}

// TopicRow is one topic's truth.
type TopicRow struct {
	Topic      string    `json:"topic"`
	LastRecord time.Time `json:"last_record"`
	// Consumers is the consumer groups holding this topic, from broker
	// metadata — evidence, not claims.
	Consumers []string `json:"consumers"`
	// Void: live producer, zero live consumer groups (refusal-class).
	Void bool `json:"void"`
	// Silent: quiet past three of its own observed cadences (refusal-class).
	Silent            bool  `json:"silent"`
	ExpectedCadenceMS int64 `json:"expected_cadence_ms,omitempty"`
	SilentForMS       int64 `json:"silent_for_ms,omitempty"`
	// OffContractRecords: traffic on this topic that the contract system
	// refused. Zero is the healthy state; nonzero names a producer that is on
	// the network without being in agreement.
	OffContractRecords int64      `json:"off_contract_records,omitempty"`
	LastOffContract    *time.Time `json:"last_off_contract,omitempty"`
}

// Finding is one refusal-class row, self-contained for the assessment tier.
type Finding struct {
	Class  string `json:"class"` // "refusal" per the RFC table; findings-class rows arrive later
	Kind   string `json:"kind"`  // "void" | "silent"
	Topic  string `json:"topic"`
	Detail string `json:"detail"`
}

// Report is the truth report. GeneratedAt stamps it; everything else is
// evidence with its source stated.
type Report struct {
	Service     string    `json:"service"`
	GeneratedAt time.Time `json:"generated_at"`
	// Authorized is the lease authority's table: who is on the network, at a
	// glance. Present when hm runs with a lease authority, absent otherwise.
	Authorized []lease.Row         `json:"authorized,omitempty"`
	Topics     []TopicRow          `json:"topics"`
	Groups     map[string][]string `json:"groups"`
	Findings   []Finding           `json:"findings"`
}

// Handler serves the truth report at request time — always current, never
// cached: the report is a statement about now, and now moves.
func Handler(src Source, led *ledger.Ledger, auth *lease.Authority) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(Build(src, led, auth, time.Now()))
	})
}

// Build assembles the report from the watcher's snapshot and the absence
// ledger, as of now.
func Build(src Source, led *ledger.Ledger, auth *lease.Authority, now time.Time) Report {
	producers, groups := src.Snapshot()
	offCounts, offLast := src.OffContract()

	// invert group->topics into topic->groups so each row carries its readers
	consumersOf := map[string][]string{}
	for group, topics := range groups {
		for _, t := range topics {
			consumersOf[t] = append(consumersOf[t], group)
		}
	}
	for _, cs := range consumersOf {
		sort.Strings(cs)
	}

	silences := map[string]ledger.Silence{}
	if led != nil {
		for _, s := range led.Silent(now) {
			silences[s.Topic] = s
		}
	}

	r := Report{Service: "hm", GeneratedAt: now, Groups: groups}
	if auth != nil {
		r.Authorized = auth.Snapshot()
		// An expired lease is a refusal the same way a void topic is: a
		// citizen on the roster that the wire says is gone.
		for _, row := range r.Authorized {
			if row.State == "expired" {
				r.Findings = append(r.Findings, Finding{
					Class: "refusal", Kind: "lease-expired", Topic: row.Service,
					Detail: "no heartbeat within three of its own observed cadences",
				})
			}
		}
	}
	topics := make([]string, 0, len(producers))
	for t := range producers {
		topics = append(topics, t)
	}
	sort.Strings(topics)

	for _, t := range topics {
		row := TopicRow{
			Topic:      t,
			LastRecord: producers[t],
			Consumers:  consumersOf[t],
			Void:       len(consumersOf[t]) == 0,
		}
		if s, quiet := silences[t]; quiet {
			row.Silent = true
			row.ExpectedCadenceMS = s.ExpectedCadence.Milliseconds()
			row.SilentForMS = s.SilentFor.Milliseconds()
		}
		if n := offCounts[t]; n > 0 {
			row.OffContractRecords = n
			if at, ok := offLast[t]; ok {
				row.LastOffContract = &at
			}
			r.Findings = append(r.Findings, Finding{
				Class: "refusal", Kind: "off-contract", Topic: t,
				Detail: fmt.Sprintf("%d records outside the contract system", n),
			})
		}
		if row.Void {
			r.Findings = append(r.Findings, Finding{
				Class: "refusal", Kind: "void", Topic: t,
				Detail: "producing with zero live consumer groups",
			})
		}
		if row.Silent {
			r.Findings = append(r.Findings, Finding{
				Class: "refusal", Kind: "silent", Topic: t,
				Detail: "quiet past three observed cadences",
			})
		}
		r.Topics = append(r.Topics, row)
	}
	return r
}
