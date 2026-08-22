package report

import (
	"encoding/json"
	"net/http"
	"sync"
)

// Gate is the /truth route while the wire comes and goes. The control port
// registers its routes once, before it serves, but the watcher behind
// /truth is built by the connect loop and rebuilt after a lost wire -- so
// the route is registered up front on this gate and the handler is swapped
// in when a watcher exists. Until then, and whenever the wire is lost, the
// route answers 503 and says so: an empty report would read as "nothing is
// talking", which is the one thing hm must never say by accident.
type Gate struct {
	mu sync.RWMutex
	h  http.Handler
}

// Set installs the live handler. Called by the connect loop on every
// (re)connection.
func (g *Gate) Set(h http.Handler) {
	g.mu.Lock()
	g.h = h
	g.mu.Unlock()
}

// Clear removes the live handler; /truth answers 503 again. Called when the
// wire is lost, before the old watcher is closed.
func (g *Gate) Clear() {
	g.Set(nil)
}

// ServeHTTP answers with the live handler, or 503 when there is none.
func (g *Gate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.mu.RLock()
	h := g.h
	g.mu.RUnlock()
	if h == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "not connected to kafka: no truth to report",
		})
		return
	}
	h.ServeHTTP(w, r)
}
