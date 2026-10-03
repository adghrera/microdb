package api

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"microdb/internal/audit"
	"microdb/internal/cluster"
	"microdb/internal/store"
)

func auditEvents(t *testing.T, path string) []audit.Event {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []audit.Event
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var e audit.Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("audit line %q: %v", line, err)
		}
		out = append(out, e)
	}
	return out
}

// TestServeHTTPAuditsMutationsAndDenials: the two events an audit log
// exists for — what was written, and what was refused.
func TestServeHTTPAuditsMutationsAndDenials(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "audit.log")
	if err := audit.Default.Open(logPath, 0); err != nil {
		t.Fatal(err)
	}
	defer func() {
		audit.Default.Close()
		// Leave the package-global sink disabled for the other tests.
		audit.Default = &audit.Log{}
	}()

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
	h := RequireAPIAuth(NewToken("s3cret"), srv)

	do := func(method, path, token, body string) int {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}

	if code := do(http.MethodPut, "/api/collections/users/docs/alice", "s3cret", `{"n":1}`); code != 200 {
		t.Fatalf("write rejected: %d", code)
	}
	if code := do(http.MethodGet, "/api/collections/users/docs/alice", "", ""); code != 401 {
		t.Fatalf("unauthenticated read: %d, want 401", code)
	}
	if code := do(http.MethodDelete, "/api/collections/users/docs/alice", "", ""); code != 401 {
		t.Fatalf("unauthenticated delete: %d, want 401", code)
	}
	// Reads must NOT be audited: they would bury the log.
	if code := do(http.MethodGet, "/api/collections/users/docs/alice", "s3cret", ""); code != 200 {
		t.Fatalf("authenticated read: %d", code)
	}

	events := auditEvents(t, logPath)
	var mutations, denials int
	for _, e := range events {
		switch {
		case e.Kind == "mutation" && e.Decision == "allow":
			mutations++
			if e.Collection != "users" || e.ID != "alice" || e.Action != http.MethodPut {
				t.Errorf("mutation recorded as %+v", e)
			}
			if e.Status != 200 {
				t.Errorf("mutation status = %d, want 200", e.Status)
			}
		case e.Kind == "auth" && e.Decision == "deny":
			denials++
			if e.Status != 401 {
				t.Errorf("denial status = %d, want 401", e.Status)
			}
			if e.Remote == "" || e.Detail == "" {
				t.Errorf("denial is missing who/why: %+v", e)
			}
		}
	}
	if mutations != 1 {
		t.Errorf("audited %d mutations, want exactly 1 (the write; reads must not be logged)", mutations)
	}
	if denials < 2 {
		t.Errorf("audited %d denials, want at least 2 (the 401s)", denials)
	}
}
