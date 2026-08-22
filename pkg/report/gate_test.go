package report

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGateAnswers503UntilSet(t *testing.T) {
	g := &Gate{}
	req := httptest.NewRequest(http.MethodGet, "/truth", nil)

	w := httptest.NewRecorder()
	g.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("empty gate answered %d, want 503", w.Code)
	}
	if !strings.Contains(w.Body.String(), "not connected") {
		t.Fatalf("503 body does not say why: %s", w.Body.String())
	}

	g.Set(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("truth"))
	}))
	w = httptest.NewRecorder()
	g.ServeHTTP(w, req)
	if w.Code != http.StatusOK || w.Body.String() != "truth" {
		t.Fatalf("set gate did not delegate: %d %s", w.Code, w.Body.String())
	}

	// a lost wire clears the gate and the route goes back to 503, never to
	// a stale report
	g.Clear()
	w = httptest.NewRecorder()
	g.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("cleared gate answered %d, want 503", w.Code)
	}
}
