package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"microdb/internal/capacity"
	"microdb/internal/cluster"
	"microdb/internal/metrics"
	"microdb/internal/store"
)

// TestCapacityShedding pins the admission contract: new writes are
// refused with 507 while the disk watermark is breached, reads and
// deletes keep working (deletes free the space the watermark is about),
// and the endpoint explains why.
func TestCapacityShedding(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cl, err := cluster.New("http://127.0.0.1:1", st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Stop()
	srv := NewWithRF("http://127.0.0.1:1", st, cl, 1)

	// Seed one document while watermarks are off.
	if _, err := st.Apply("cap", "keep", map[string]interface{}{"n": 1}); err != nil {
		t.Fatal(err)
	}

	do := func(method, path string) (int, string) {
		r := httptest.NewRequest(method, path, strings.NewReader(`{"n":2}`))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, r)
		return w.Code, w.Body.String()
	}

	// 101% free required: nobody has that, so the shed is deterministic.
	srv.SetCapacity(&capacity.Checker{Path: dir, MinFreePct: 101, Every: time.Millisecond})
	shedsBefore := atomic.LoadInt64(metrics.Default.Counter("microdb_write_sheds_total", "x"))

	if code, body := do(http.MethodPut, "/api/collections/cap/docs/new"); code != 507 {
		t.Fatalf("write under watermark = %d, want 507 (%s)", code, body)
	}
	var shed struct {
		Error  string `json:"error"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal([]byte(do2(t, srv, http.MethodPut, "/api/collections/cap/docs/new")), &shed); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(shed.Reason, "disk free") || !strings.Contains(shed.Error, "insufficient capacity") {
		t.Errorf("shed response should explain, got %+v", shed)
	}
	if got := atomic.LoadInt64(metrics.Default.Counter("microdb_write_sheds_total", "x")); got != shedsBefore+2 {
		t.Errorf("shed metric = %d, want %d", got, shedsBefore+2)
	}

	// Reads keep working: shedding is about accepting NEW data.
	if code, _ := do(http.MethodGet, "/api/collections/cap/docs/keep"); code != 200 {
		t.Errorf("read under watermark = %d, want 200", code)
	}
	// Deletes keep working: they free the space we are short of.
	if code, _ := do(http.MethodDelete, "/api/collections/cap/docs/keep"); code != 200 {
		t.Errorf("delete under watermark = %d, want 200", code)
	}

	// The endpoint explains itself.
	r := httptest.NewRequest(http.MethodGet, "/api/capacity", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("/api/capacity -> %d", w.Code)
	}
	var out struct {
		Capacity struct {
			Disk struct {
				TotalBytes uint64 `json:"total_bytes"`
			} `json:"disk"`
			ShedReason string `json:"shed_reason"`
			Docs       int    `json:"docs"`
		} `json:"capacity"`
		Ready bool `json:"ready"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("capacity response: %v (%s)", err, w.Body.String())
	}
	if out.Capacity.Disk.TotalBytes == 0 {
		t.Error("capacity endpoint reports no disk numbers")
	}
	if out.Capacity.ShedReason == "" || out.Ready {
		t.Errorf("endpoint should report shedding and not-ready: %+v", out)
	}
	if out.Capacity.Docs < 1 {
		t.Errorf("docs = %d, want the seeded document", out.Capacity.Docs)
	}

	// Watermarks off: writes flow again.
	srv.SetCapacity(nil)
	if code, _ := do(http.MethodPut, "/api/collections/cap/docs/new"); code != 200 {
		t.Errorf("write after disabling watermarks = %d, want 200", code)
	}
}

// do2 performs a request and returns the body (for response parsing).
func do2(t *testing.T, srv *Server, method, path string) string {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(`{"n":3}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Body.String()
}
