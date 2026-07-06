// Package server is hm's control port: /health and /metrics, the two
// endpoints every fleet daemon owes the mesh. Nothing else lives here; hm's
// verdict surfaces arrive with the truth report, not before.
package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
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
	log   *slog.Logger
	start time.Time

	// setDegraded is written by the owning process when a dependency is down
	// (e.g. the broker is unreachable); read by /health. Guarded by the
	// httpapi goroutine being the only reader and Set being atomic enough for
	// a status string is NOT assumed: a mutex would be overkill for v0's
	// single writer at startup, but the field is private and mutated only via
	// SetDegraded before Serve, so the race window is nil today. Revisit when
	// runtime degradation (broker loss mid-flight) starts writing it.
	degraded string
}

// New builds the control port on addr.
func New(addr string, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{log: log, start: time.Now()}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.Handle("/metrics", metrics.Handler())
	s.http = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return s
}

// SetDegraded marks the health surface degraded with a human-readable reason.
// Call before Serve; see the field comment for the mid-flight caveat.
func (s *Server) SetDegraded(reason string) {
	s.degraded = reason
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	h := Health{
		Service:       "hm",
		Status:        "ok",
		UptimeSeconds: int64(time.Since(s.start).Seconds()),
	}
	if s.degraded != "" {
		h.Status = "degraded"
		h.Detail = s.degraded
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(h)
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
