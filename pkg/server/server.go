// Package server is hm's control port: /health, /live and /metrics, the
// endpoints every fleet daemon owes the mesh. Nothing else lives here; hm's
// verdict surfaces arrive with the truth report, not before.
package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/janearc/hall-monitor/pkg/metrics"
)

// Health is the /health payload. Degraded states are reported, never hidden:
// hm holds other services to that standard and is held to it first.
type Health struct {
	Service       string `json:"service"`
	Status        string `json:"status"` // "ok" or "degraded"
	Detail        string `json:"detail,omitempty"`
	UptimeSeconds int64  `json:"uptime_seconds"`
}

// Server owns the HTTP listener and the health state it reports.
type Server struct {
	http  *http.Server
	mux   *http.ServeMux
	log   *slog.Logger
	start time.Time

	// degraded is a bool because degraded IS a bool; reason is the separate
	// human-readable why. A degraded state without a reason is banned (fail
	// loud means saying what failed), which SetDegraded enforces by taking
	// the reason as its argument. Mutex-guarded because the connect loop
	// writes it on every wire transition while /health reads.
	mu       sync.RWMutex
	degraded bool
	reason   string
}

// New builds the control port on addr.
func New(addr string, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{log: log, start: time.Now()}
	s.mux = http.NewServeMux()
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/live", s.handleLive)
	s.mux.Handle("/metrics", metrics.Handler())
	s.http = &http.Server{Addr: addr, Handler: s.mux, ReadHeaderTimeout: 5 * time.Second}
	return s
}

// Handle registers an additional route on the control port. MUST be called
// before Serve.
func (s *Server) Handle(pattern string, h http.Handler) {
	s.mux.Handle(pattern, h)
}

// SetDegraded marks the health surface degraded with a human-readable reason.
func (s *Server) SetDegraded(reason string) {
	s.mu.Lock()
	s.degraded = true
	s.reason = reason
	s.mu.Unlock()
}

// SetOK clears the degraded state. The connect loop calls it when the wire
// is (re)established; nothing else should, because nothing else knows.
func (s *Server) SetOK() {
	s.mu.Lock()
	s.degraded = false
	s.reason = ""
	s.mu.Unlock()
}

// handleHealth serves the /health payload. Degraded is 503 WITH the body,
// not 200 with a sad status: a probe or a scraper that reads only the code
// must see the truth too. For forty minutes on 2026-08-22 a 200 here kept
// a blind hm "1/1 Running" while it watched nothing; the code is the part
// of this answer that kubernetes reads.
func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	h := Health{
		Service:       "hm",
		Status:        "ok",
		UptimeSeconds: int64(time.Since(s.start).Seconds()),
	}
	code := http.StatusOK
	s.mu.RLock()
	if s.degraded {
		h.Status = "degraded"
		h.Detail = s.reason
		code = http.StatusServiceUnavailable
	}
	s.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(h)
}

// handleLive answers 200 whenever the process can answer at all. It is the
// liveness probe's target and deliberately knows nothing about the wire: a
// degraded hm is RETRYING, and restarting a process that is correctly
// retrying does not bring the broker back -- it only resets the backoff and
// throws away the attempt count. Readiness is /health; liveness is this.
func (s *Server) handleLive(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"service":        "hm",
		"alive":          true,
		"uptime_seconds": int64(time.Since(s.start).Seconds()),
	})
}

// Serve blocks until ctx is cancelled, then drains with a short grace period.
func (s *Server) Serve(ctx context.Context) error {
	errc := make(chan error, 1)
	go func() { errc <- s.http.ListenAndServe() }()
	s.log.Info("control port up", "addr", s.http.Addr)
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.http.Shutdown(shutdownCtx)
	}
}
