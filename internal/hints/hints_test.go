package hints

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"microdb/internal/store"
)

func doc(col, id string, ver int64) *store.Doc {
	return &store.Doc{ID: id, Ver: ver, TS: time.Now().UnixMilli(), Collection: col,
		Fields: map[string]interface{}{"v": ver}}
}

func TestAddAndDue(t *testing.T) {
	s, err := Load(filepath.Join(t.TempDir(), "h.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	s.Add("http://b:1", doc("c", "k1", 1))
	s.Add("http://b:1", doc("c", "k2", 1))
	s.Add("http://b:2", doc("c", "k3", 1))
	if s.Pending() != 3 {
		t.Fatalf("pending=%d want 3", s.Pending())
	}
	due := s.Due(time.Now())
	if len(due) != 3 {
		t.Fatalf("due=%d want 3 (fresh hints are immediately due)", len(due))
	}
	// Nothing due in the past.
	if got := s.Due(time.Now().Add(-time.Hour)); len(got) != 0 {
		t.Fatalf("due in past=%d want 0", len(got))
	}
}

func TestAddReplacesSameTargetDoc(t *testing.T) {
	s, _ := Load(filepath.Join(t.TempDir(), "h.jsonl"))
	s.Add("http://b:1", doc("c", "k", 1))
	s.Add("http://b:1", doc("c", "k", 2)) // newer version of same owed doc
	if s.Pending() != 1 {
		t.Fatalf("pending=%d want 1 (replace, not duplicate)", s.Pending())
	}
	due := s.Due(time.Now())
	if due[0].Doc.Ver != 2 {
		t.Fatalf("stored ver=%d want 2 (newest wins)", due[0].Doc.Ver)
	}
	// Same doc id to a DIFFERENT target stays separate.
	s.Add("http://b:2", doc("c", "k", 1))
	if s.Pending() != 2 {
		t.Fatalf("pending=%d want 2", s.Pending())
	}
}

func TestRemoveDelivered(t *testing.T) {
	s, _ := Load(filepath.Join(t.TempDir(), "h.jsonl"))
	s.Add("http://b:1", doc("c", "k1", 1))
	if !s.Remove("http://b:1", "c", "k1") {
		t.Fatal("remove should find the hint")
	}
	if s.Pending() != 0 {
		t.Fatalf("pending=%d want 0", s.Pending())
	}
	if s.Remove("http://b:1", "c", "k1") {
		t.Fatal("double-remove must report false")
	}
	_, _, delivered, _, _ := s.Stats()
	if delivered != 1 {
		t.Fatalf("delivered=%d want 1", delivered)
	}
}

func TestRescheduleBackoff(t *testing.T) {
	s, _ := Load(filepath.Join(t.TempDir(), "h.jsonl"))
	s.Add("http://b:1", doc("c", "k", 1))
	s.Reschedule("http://b:1", "c", "k", 5*time.Minute)
	// After one failure the hint is NOT due immediately.
	if due := s.Due(time.Now()); len(due) != 0 {
		t.Fatal("hint should be backed off after failure")
	}
	// It becomes due after the first backoff window (2s).
	if due := s.Due(time.Now().Add(3 * time.Second)); len(due) != 1 {
		t.Fatal("hint should be due after backoff elapses")
	}
	// Repeated failures grow the delay but cap at maxBackoff.
	for i := 0; i < 12; i++ {
		s.Reschedule("http://b:1", "c", "k", 5*time.Minute)
	}
	due := s.Due(time.Now().Add(5*time.Minute + time.Second))
	if len(due) != 1 {
		t.Fatal("hint must be due within maxBackoff")
	}
	// (NextRetry is now+5min exactly, so just before that it's not due.)
	if due := s.Due(time.Now().Add(5*time.Minute - time.Second)); len(due) != 0 {
		t.Fatal("hint must not be due before maxBackoff elapses")
	}
}

func TestCapDropsOldest(t *testing.T) {
	s, _ := Load(filepath.Join(t.TempDir(), "h.jsonl"))
	s.max = 3
	for i := 0; i < 5; i++ {
		s.Add("http://b:1", doc("c", string(rune('a'+i)), 1))
	}
	if s.Pending() != 3 {
		t.Fatalf("pending=%d want 3 (cap)", s.Pending())
	}
	_, _, _, dropped, _ := s.Stats()
	if dropped != 2 {
		t.Fatalf("dropped=%d want 2", dropped)
	}
	// The two oldest (a, b) are gone; c, d, e remain.
	due := s.Due(time.Now())
	ids := map[string]bool{}
	for _, h := range due {
		ids[h.Doc.ID] = true
	}
	if ids["a"] || ids["b"] || !ids["c"] || !ids["d"] || !ids["e"] {
		t.Fatalf("wrong survivors: %v", ids)
	}
}

func TestPersistenceRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.jsonl")
	s, _ := Load(path)
	s.Add("http://b:1", doc("c", "k1", 7))
	s.Add("http://b:2", doc("c", "k2", 8))
	s.Reschedule("http://b:2", "c", "k2", time.Minute) // also persists attempts

	// Reload from disk: both hints survive with their state.
	s2, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if s2.Pending() != 2 {
		t.Fatalf("after reload pending=%d want 2", s2.Pending())
	}
	// k2 was backed off: not due now, due in a minute.
	if due := s2.Due(time.Now()); len(due) != 1 || due[0].Doc.ID != "k1" {
		t.Fatalf("backoff state lost across reload: %+v", due)
	}
	if due := s2.Due(time.Now().Add(2 * time.Minute)); len(due) != 2 {
		t.Fatalf("both hints should be due eventually: %+v", due)
	}
}

func TestLoadSkipsCorruptLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.jsonl")
	s, _ := Load(path)
	s.Add("http://b:1", doc("c", "k1", 1))
	// Simulate a crash mid-write: append garbage.
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	f.WriteString(`{"target":"http://b:1","doc":{trunca`)
	f.Close()
	s2, err := Load(path)
	if err != nil {
		t.Fatalf("corrupt tail must not fail load: %v", err)
	}
	if s2.Pending() != 1 {
		t.Fatalf("pending=%d want 1 (good line kept, garbage skipped)", s2.Pending())
	}
}
