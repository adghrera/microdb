package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTokenRotation(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	tok := NewToken("first")
	h := RequireAPIAuth(tok, inner)

	get := func(token string) int {
		r := httptest.NewRequest(http.MethodGet, "/api/anything", nil)
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}

	if got := get("first"); got != 200 {
		t.Fatalf("initial token rejected: %d", got)
	}
	if got := get("second"); got != 401 {
		t.Fatalf("wrong token accepted before rotation: %d", got)
	}

	// Rotation takes effect on the next request, no restart.
	tok.Set("second")
	if got := get("second"); got != 200 {
		t.Fatalf("rotated token rejected: %d", got)
	}
	if got := get("first"); got != 401 {
		t.Fatalf("old token still accepted after rotation: %d", got)
	}

	// Clearing the token reopens the API (documented behaviour of "").
	tok.Set("")
	if got := get(""); got != 200 {
		t.Fatalf("cleared token should disable auth, got %d", got)
	}

	// A nil Token must never panic: guards the wiring path.
	var nilTok *Token
	if nilTok.Get() != "" {
		t.Error("nil token should read as empty")
	}
	nilTok.Set("x") // no-op
}

func TestTokenLeavesPublicPathsAlone(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := RequireAPIAuth(NewToken("secret"), inner)
	for _, path := range []string{"/health", "/version", "/metrics"} {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Errorf("%s -> %d, want 200 without a token", path, w.Code)
		}
	}
}
