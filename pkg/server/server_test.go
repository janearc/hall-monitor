package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func health(t *testing.T, s *Server) (int, Health) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	s.handleHealth(w, req)
	var h Health
	if err := json.NewDecoder(w.Result().Body).Decode(&h); err != nil {
		t.Fatalf("health not JSON: %v", err)
	}
	return w.Code, h
}

func TestHealthOKDegradedAndBack(t *testing.T) {
	s := New(":0", nil)

	code, h := health(t, s)
	if code != http.StatusOK || h.Status != "ok" || h.Service != "hm" {
		t.Fatalf("fresh server not ok: %d %+v", code, h)
	}

	// degraded is reported, never hidden -- and the status CODE says it,
	// because the probe reads the code and not the body
	s.SetDegraded("no eyes")
	code, h = health(t, s)
	if code != http.StatusServiceUnavailable || h.Status != "degraded" || h.Detail != "no eyes" {
		t.Fatalf("degraded state hidden: %d %+v", code, h)
	}

	// the wire comes back: ok again, reason gone
	s.SetOK()
	code, h = health(t, s)
	if code != http.StatusOK || h.Status != "ok" || h.Detail != "" {
		t.Fatalf("recovery not reported: %d %+v", code, h)
	}
}

func TestLiveIgnoresTheWire(t *testing.T) {
	s := New(":0", nil)
	s.SetDegraded("kafka gone")
	req := httptest.NewRequest(http.MethodGet, "/live", nil)
	w := httptest.NewRecorder()
	s.handleLive(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("/live answered %d while degraded; liveness must not restart a retrying hm", w.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(w.Result().Body).Decode(&body); err != nil {
		t.Fatalf("live not JSON: %v", err)
	}
	if body["alive"] != true {
		t.Fatalf("live body does not say alive: %v", body)
	}
}
