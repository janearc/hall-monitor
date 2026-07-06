package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCountersServeSorted(t *testing.T) {
	Inc(`hm_test_total{k="b"}`)
	Add(`hm_test_total{k="a"}`, 2)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	Handler().ServeHTTP(w, req)

	body, _ := io.ReadAll(w.Result().Body)
	out := string(body)
	if !strings.Contains(out, `hm_test_total{k="a"} 2`) || !strings.Contains(out, `hm_test_total{k="b"} 1`) {
		t.Fatalf("counters missing or wrong: %s", out)
	}
	if strings.Index(out, `k="a"`) > strings.Index(out, `k="b"`) {
		t.Fatalf("output not sorted: %s", out)
	}
}
