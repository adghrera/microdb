package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLogRecordsCarryChecksums: every record written must be wrapped
// in a CRC32C envelope that verifies.
func TestLogRecordsCarryChecksums(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := st.Apply("c", docName(i), map[string]interface{}{"n": i}); err != nil {
			t.Fatal(err)
		}
	}
	st.Close()

	raw, err := os.ReadFile(filepath.Join(dir, "data.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 5 {
		t.Fatalf("expected 5 records, got %d", len(lines))
	}
	for i, l := range lines {
		if !strings.HasPrefix(l, crcPrefix) {
			t.Fatalf("record %d has no checksum envelope: %.40s", i, l)
		}
		if _, ok := verifyRecord(l); !ok {
			t.Fatalf("record %d fails its own checksum: %.60s", i, l)
		}
	}
}

// TestCorruptRecordDetectedOnReplay flips a byte in the MIDDLE of the
// log. The store must still open, the damaged record must be dropped
// (not silently accepted as data), and everything else must survive.
func TestCorruptRecordDetectedOnReplay(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := st.Apply("c", docName(i), map[string]interface{}{"n": i}); err != nil {
			t.Fatal(err)
		}
	}
	st.Close()

	path := filepath.Join(dir, "data.jsonl")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(data), "\n")
	// Corrupt the payload of record 5 (flip a digit inside the JSON).
	victim := 5
	if !strings.Contains(lines[victim], `"n":5`) {
		t.Fatalf("expected record 5 to hold n=5: %.80s", lines[victim])
	}
	lines[victim] = strings.Replace(lines[victim], `"n":5`, `"n":9`, 1)
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o644); err != nil {
		t.Fatal(err)
	}

	st2, err := Open(dir)
	if err != nil {
		t.Fatalf("corrupt record must not make the store unopenable: %v", err)
	}
	defer st2.Close()
	if d, ok := st2.Get("c", docName(5)); ok {
		t.Fatalf("corrupt record was accepted as data: %#v", d.Fields)
	}
	for i := 0; i < 10; i++ {
		if i == 5 {
			continue
		}
		if _, ok := st2.Get("c", docName(i)); !ok {
			t.Fatalf("clean record %d lost alongside the corrupt one", i)
		}
	}
}

// TestTornTailTolerated: a crash mid-append leaves half a line at EOF.
// That must be treated as a torn write, not as corruption.
func TestTornTailTolerated(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if _, err := st.Apply("c", docName(i), map[string]interface{}{"n": i}); err != nil {
			t.Fatal(err)
		}
	}
	st.Close()

	path := filepath.Join(dir, "data.jsonl")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Chop the last record in half (simulates dying mid-write).
	if err := os.WriteFile(path, data[:len(data)-40], 0o644); err != nil {
		t.Fatal(err)
	}

	st2, err := Open(dir)
	if err != nil {
		t.Fatalf("torn tail must be tolerated: %v", err)
	}
	defer st2.Close()
	for i := 0; i < 5; i++ {
		if _, ok := st2.Get("c", docName(i)); !ok {
			t.Fatalf("record %d lost to the torn tail", i)
		}
	}
}

func TestVerifyLogReportsDamage(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if _, err := st.Apply("c", docName(i), map[string]interface{}{"n": i}); err != nil {
			t.Fatal(err)
		}
	}
	st.Close()
	path := filepath.Join(dir, "data.jsonl")

	rep, err := VerifyLog(path)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK() || rep.CorruptCount != 0 {
		t.Fatalf("clean log reported damaged: %+v", rep)
	}
	if rep.Records != 8 || rep.ChecksummedRecords != 8 {
		t.Fatalf("records=%d checksummed=%d, want 8/8", rep.Records, rep.ChecksummedRecords)
	}

	// Torn tail: tolerated, flagged, still OK.
	data, _ := os.ReadFile(path)
	if err := os.WriteFile(path, data[:len(data)-30], 0o644); err != nil {
		t.Fatal(err)
	}
	rep, err = VerifyLog(path)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK() || !rep.TornTail {
		t.Fatalf("torn tail should be OK with TornTail set: %+v", rep)
	}

	// Middle damage: reported, not OK, with a byte offset that points
	// at the record so an operator can find it.
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	data, _ = os.ReadFile(path)
	lines := strings.Split(string(data), "\n")
	lines[3] = strings.Replace(lines[3], `"n":3`, `"n":7`, 1)
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o644); err != nil {
		t.Fatal(err)
	}
	rep, err = VerifyLog(path)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK() {
		t.Fatal("corrupt record not reported")
	}
	if rep.CorruptCount != 1 || len(rep.Corrupt) != 1 {
		t.Fatalf("want exactly 1 corrupt record, got %d", rep.CorruptCount)
	}
	if rep.Corrupt[0].Line != 4 {
		t.Errorf("reported line %d, want 4", rep.Corrupt[0].Line)
	}
	if rep.TornTail {
		t.Error("middle damage must not be excused as a torn tail")
	}
}
