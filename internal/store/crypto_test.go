package store

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestEncryptionRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenWithKey(dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	st.Apply("c", "secret", map[string]interface{}{"v": "classified"})
	st.Apply("c", "secret", map[string]interface{}{"v": "classified-v2"})
	st.Delete("c", "gone")
	st.Close()

	// The raw log must contain NO plaintext of the values.
	raw, _ := os.ReadFile(filepath.Join(dir, "data.jsonl"))
	text := string(raw)
	if strings.Contains(text, "classified") {
		t.Fatal("plaintext leaked into encrypted log")
	}
	// Records are CRC-wrapped, then encrypted: the envelope sits
	// outside the ciphertext so rot can be spotted without a key.
	firstLine := strings.SplitN(text, "\n", 2)[0]
	payload, intact := verifyRecord(firstLine)
	if !intact {
		t.Fatalf("first log record failed its checksum: %.60s", firstLine)
	}
	if !strings.HasPrefix(payload, encPrefix) {
		t.Fatalf("log records not in ENC1 form: %.60s", payload)
	}

	// Reopen with the right key: full state recovered.
	st2, err := OpenWithKey(dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	d, ok := st2.Get("c", "secret")
	if !ok || d.Fields["v"] != "classified-v2" {
		t.Fatalf("decrypted state wrong: %v", d)
	}
	if d, ok := st2.Get("c", "gone"); ok && !d.Deleted {
		t.Fatal("tombstone lost through encryption")
	}
}

func TestWrongKeyFailsLoud(t *testing.T) {
	dir := t.TempDir()
	st, _ := OpenWithKey(dir, testKey)
	st.Apply("c", "k", map[string]interface{}{"v": 1})
	st.Close()

	wrongKey := strings.Repeat("ab", 32)
	_, err := OpenWithKey(dir, wrongKey)
	if err == nil {
		t.Fatal("wrong key must fail replay, not silently return empty")
	}
	// No key at all must also fail (record is ENC1).
	_, err = OpenWithKey(dir, "")
	if err == nil {
		t.Fatal("missing key on encrypted log must fail")
	}
}

func TestKeyValidation(t *testing.T) {
	for _, bad := range []string{"abc", strings.Repeat("zz", 32), strings.Repeat("00", 31)} {
		_, err := OpenWithKey(t.TempDir(), bad)
		if err == nil {
			t.Fatalf("bad key %q accepted", bad)
		}
	}
}

func TestEncryptionMixedLog(t *testing.T) {
	// A plaintext db that later gets encrypted: old plaintext records
	// and new ENC1 records coexist and both replay.
	dir := t.TempDir()
	st, _ := Open(dir)
	st.Apply("c", "old", map[string]interface{}{"v": "plain-era"})
	st.Close()

	st2, err := OpenWithKey(dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	st2.Apply("c", "new", map[string]interface{}{"v": "crypto-era"})
	st2.Close()

	raw, _ := os.ReadFile(filepath.Join(dir, "data.jsonl"))
	text := string(raw)
	if !strings.Contains(text, "plain-era") {
		t.Fatal("old plaintext record vanished")
	}
	if !strings.Contains(text, encPrefix) {
		t.Fatal("new record not encrypted")
	}
	// Reopen with key: both eras readable.
	st3, err := OpenWithKey(dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	defer st3.Close()
	if d, ok := st3.Get("c", "old"); !ok || d.Fields["v"] != "plain-era" {
		t.Fatalf("old record unreadable: %v", d)
	}
	if d, ok := st3.Get("c", "new"); !ok || d.Fields["v"] != "crypto-era" {
		t.Fatalf("new record unreadable: %v", d)
	}
}

func TestCompactionEncryptsEverything(t *testing.T) {
	// After compaction with a key set, even old plaintext-era records
	// are rewritten encrypted.
	dir := t.TempDir()
	st, _ := Open(dir)
	st.Apply("c", "legacy", map[string]interface{}{"v": "plain-legacy"})
	st.Close()

	st2, _ := OpenWithKey(dir, testKey)
	st2.Apply("c", "fresh", map[string]interface{}{"v": "crypto"})
	if _, err := st2.Compact(time.Hour); err != nil {
		t.Fatal(err)
	}
	st2.Close()

	raw, _ := os.ReadFile(filepath.Join(dir, "data.jsonl"))
	if strings.Contains(string(raw), "plain-legacy") {
		t.Fatal("compaction left plaintext behind")
	}
	st3, err := OpenWithKey(dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	defer st3.Close()
	if d, ok := st3.Get("c", "legacy"); !ok || d.Fields["v"] != "plain-legacy" {
		t.Fatalf("legacy record lost after encrypted compaction: %v", d)
	}
}

func TestEncryptedPITR(t *testing.T) {
	// Archive an encrypted db, PITR with the key restores state.
	src, _ := OpenWithKey(t.TempDir(), testKey)
	src.Apply("c", "k", map[string]interface{}{"v": "before"})
	time.Sleep(10 * time.Millisecond)
	t1 := time.Now().UnixMilli()
	time.Sleep(10 * time.Millisecond)
	src.Apply("c", "k", map[string]interface{}{"v": "after"})
	var raw bytes.Buffer
	if err := src.ArchiveRaw(&raw); err != nil {
		t.Fatal(err)
	}
	src.Close()

	// Archive is encrypted (no plaintext).
	if strings.Contains(raw.String(), "before") || strings.Contains(raw.String(), "after") {
		t.Fatal("archive leaked plaintext")
	}
	// PITR without key fails.
	if _, _, err := ReplayUntil(bytes.NewReader(raw.Bytes()), t1, t.TempDir()); err == nil {
		t.Fatal("keyless PITR of encrypted archive must fail")
	}
	// PITR with key works and respects the timestamp.
	rec, n, err := ReplayUntilKey(bytes.NewReader(raw.Bytes()), t1, t.TempDir(), testKey)
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 {
		t.Fatal("nothing replayed")
	}
	defer rec.Close()
	d, ok := rec.Get("c", "k")
	if !ok || d.Fields["v"] != "before" {
		t.Fatalf("encrypted PITR wrong state: %v", d)
	}
}

func TestEncryptedBackupRestore(t *testing.T) {
	// Backup produces plaintext JSONL (current state) even from an
	// encrypted store; restore into a plaintext store works.
	src, _ := OpenWithKey(t.TempDir(), testKey)
	src.Apply("c", "k", map[string]interface{}{"v": "secure"})
	var buf bytes.Buffer
	if err := src.Backup(&buf); err != nil {
		t.Fatal(err)
	}
	src.Close()
	if !strings.Contains(buf.String(), "secure") {
		t.Fatal("backup should be readable JSON (decrypted)")
	}
	dst, _ := Open(t.TempDir())
	defer dst.Close()
	n, err := dst.Restore(bytes.NewReader(buf.Bytes()))
	if err != nil || n != 1 {
		t.Fatalf("restore: n=%d err=%v", n, err)
	}
	d, ok := dst.Get("c", "k")
	if !ok || d.Fields["v"] != "secure" {
		t.Fatalf("restored wrong: %v", d)
	}
	_ = json.Marshal
}
