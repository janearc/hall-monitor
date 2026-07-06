// Package metrics is hm's internal counter surface, served at /metrics in
// prometheus text exposition format. It is deliberately tiny: hm's own
// telemetry must not depend on anything heavier than the standard library,
// because hm is the thing that watches everything else.
//
// NOTE (platform gap, flagged at v0): every daemon in this fleet needs this
// exact surface, and today each service hand-writes its own because the frood
// library does not provide one. That is the copy-pressure the no-copies rule
// exists to surface; a frood-lib metrics package is the fix, tracked as an
// issue, and this package dies the day it exists.
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
)

var (
	mu       sync.RWMutex
	counters = map[string]int64{}
)

// Inc adds one to the named counter. Names follow prometheus conventions,
// labels inline: hm_records_consumed_total{topic="delight.events"}.
func Inc(name string) {
	Add(name, 1)
}

// Add adds n to the named counter.
func Add(name string, n int64) {
	mu.Lock()
	counters[name] += n
	mu.Unlock()
}

// Handler serves the registry in prometheus text format, keys sorted so the
// output is diffable by eye and by machine.
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.RLock()
		names := make([]string, 0, len(counters))
		for name := range counters {
			names = append(names, name)
		}
		sort.Strings(names)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		for _, name := range names {
			fmt.Fprintf(w, "%s %d\n", name, counters[name])
		}
		mu.RUnlock()
	})
}
