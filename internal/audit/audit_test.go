package audit

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func readEvents(t *testing.T, path string) []Event {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []Event
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var e Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("audit line %q is not JSON: %v", line, err)
		}
		out = append(out, e)
	}
	return out
}

func TestDisabledLogDropsEverything(t *testing.T) {
	l := &Log{}
	if l.Enabled() {
		t.Fatal("zero value must be disabled")
	}
	l.Log(Event{Kind: "auth"}) // must not panic
	if l.Path() != "" {
		t.Error("disabled log should have no path")
	}
	if err := l.Close(); err != nil {
		t.Errorf("close on disabled log: %v", err)
	}
}

func TestLogWritesJSONLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	l := &Log{}
	if err := l.Open(path, 0); err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if !l.Enabled() {
		t.Fatal("open should enable the log")
	}
	l.Log(Event{Kind: "auth", Decision: "deny", Status: 401, Remote: "10.0.0.9", Detail: "invalid bearer token"})
	l.Log(Event{Kind: "mutation", Action: "PUT", Decision: "allow", Collection: "users", ID: "alice", Status: 200})

	got := readEvents(t, path)
	if len(got) != 2 {
		t.Fatalf("events = %d, want 2", len(got))
	}
	if got[0].Decision != "deny" || got[0].Status != 401 || got[0].Remote != "10.0.0.9" {
		t.Errorf("denial recorded as %+v", got[0])
	}
	if got[1].Collection != "users" || got[1].ID != "alice" {
		t.Errorf("mutation recorded as %+v", got[1])
	}
	if got[0].TS == 0 || got[1].TS == 0 {
		t.Error("timestamps must be filled in")
	}
}

// TestLogRotatesBounded: the audit log must not be able to fill the
// disk it is protecting — one generation, capped size.
func TestLogRotatesBounded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	l := &Log{}
	// Tiny cap so the test exercises rotation immediately.
	if err := l.Open(path, 512); err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	for i := 0; i < 200; i++ {
		l.Log(Event{Kind: "mutation", Action: "PUT", Collection: "c", ID: "k", Detail: "padding padding padding padding"})
	}
	if _, err := os.Stat(path + ".1"); err != nil {
		t.Fatalf("rotation did not happen: %v", err)
	}
	cur, _ := os.Stat(path)
	old, _ := os.Stat(path + ".1")
	if cur.Size() > 1024 {
		t.Errorf("current log is %d bytes, cap was 512", cur.Size())
	}
	if old.Size() > 1024 {
		t.Errorf("previous generation is %d bytes", old.Size())
	}
	// Both files must still be valid JSONL (rotation cannot corrupt a
	// record it just wrote).
	for _, p := range []string{path, path + ".1"} {
		if _, err := os.Stat(p); err != nil {
			continue
		}
		readEvents(t, p)
	}
}
