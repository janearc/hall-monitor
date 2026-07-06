package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthOKAndDegraded(t *testing.T) {
	s := New(":0", nil)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	s.handleHealth(w, req)
	var h Health
	if err := json.NewDecoder(w.Result().Body).Decode(&h); err != nil {
		t.Fatalf("health not JSON: %v", err)
	}
	if h.Status != "ok" || h.Service != "hm" {
		t.Fatalf("fresh server not ok: %+v", h)
	}

	// degraded is reported, never hidden
	s.SetDegraded("no eyes")
	w = httptest.NewRecorder()
	s.handleHealth(w, req)
	if err := json.NewDecoder(w.Result().Body).Decode(&h); err != nil {
		t.Fatalf("health not JSON: %v", err)
	}
	if h.Status != "degraded" || h.Detail != "no eyes" {
		t.Fatalf("degraded state hidden: %+v", h)
	}
}
