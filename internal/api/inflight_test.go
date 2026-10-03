package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestInflightTracksActiveRequests is what graceful shutdown waits on:
// the counter must be non-zero exactly while a handler runs.
func TestInflightTracksActiveRequests(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		w.WriteHeader(http.StatusOK)
	})
	h := Tracing(10000, inner)

	if got := Inflight(); got != 0 {
		t.Fatalf("inflight before any request = %d, want 0", got)
	}
	done := make(chan int)
	go func() {
		r := httptest.NewRequest(http.MethodGet, "/health", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		done <- w.Code
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never ran")
	}
	// Give the counter its increment a moment (it happens before the
	// handler body, but the channel wake-up can race by a hair).
	deadline := time.Now().Add(time.Second)
	for Inflight() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := Inflight(); got != 1 {
		t.Errorf("inflight during a request = %d, want 1", got)
	}

	close(release)
	select {
	case code := <-done:
		if code != 200 {
			t.Fatalf("handler returned %d", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler never returned")
	}
	deadline = time.Now().Add(time.Second)
	for Inflight() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := Inflight(); got != 0 {
		t.Errorf("inflight after completion = %d, want 0 (shutdown would wait forever)", got)
	}
}
