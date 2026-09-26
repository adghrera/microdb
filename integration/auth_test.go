package integration

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"microdb/internal/api"
)

func TestRequireAPIAuth(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	})
	h := api.RequireAPIAuth("s3cret", inner)

	do := func(path, auth string) int {
		req := httptest.NewRequest("GET", path, nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}

	if c := do("/api/collections/x/docs", ""); c != 401 {
		t.Fatalf("no token should be 401, got %d", c)
	}
	if c := do("/api/collections/x/docs", "Bearer wrong"); c != 401 {
		t.Fatalf("wrong token should be 401, got %d", c)
	}
	if c := do("/api/collections/x/docs", "Bearer s3cret"); c != 200 {
		t.Fatalf("good token should be 200, got %d", c)
	}
	if c := do("/health", ""); c != 200 {
		t.Fatalf("/health must stay open, got %d", c)
	}
	if c := do("/internal/scan/x", ""); c != 200 {
		t.Fatalf("auth middleware should not gate /internal (TLS middleware does), got %d", c)
	}
	// Empty token disables auth entirely.
	open := api.RequireAPIAuth("", inner)
	req := httptest.NewRequest("GET", "/api/collections/x/docs", nil)
	rec := httptest.NewRecorder()
	open.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("empty token should disable auth, got %d", rec.Code)
	}
	_ = strings.TrimSpace
}
