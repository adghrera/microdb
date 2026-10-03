package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func logBytes(t *testing.T, dir string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "data.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestCompressionShrinksTheLog is the feature's whole claim: same
// data, fewer bytes, and the bytes still come back.
func TestCompressionShrinksTheLog(t *testing.T) {
	pad := strings.Repeat("the same bytes over and over ", 40) // ~1.1KB, very compressible

	plainDir := t.TempDir()
	st, err := Open(plainDir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := st.Apply("c", docName(i), map[string]interface{}{"n": i, "pad": pad}); err != nil {
			t.Fatal(err)
		}
	}
	st.Close()

	compDir := t.TempDir()
	st2, err := Open(compDir)
	if err != nil {
		t.Fatal(err)
	}
	st2.SetCompress(true)
	for i := 0; i < 20; i++ {
		if _, err := st2.Apply("c", docName(i), map[string]interface{}{"n": i, "pad": pad}); err != nil {
			t.Fatal(err)
		}
	}
	st2.Close()

	plainSize := len(logBytes(t, plainDir))
	compSize := len(logBytes(t, compDir))
	t.Logf("log size: %d plain -> %d compressed (%.0f%% smaller)",
		plainSize, compSize, 100*(1-float64(compSize)/float64(plainSize)))
	if compSize >= plainSize*3/4 {
		t.Errorf("compression saved almost nothing: %d -> %d", plainSize, compSize)
	}
}

// TestCompressedLogReadsWithCompressionOff: turning the flag off must
// never strand a log written with it on.
func TestCompressedLogReadsWithCompressionOff(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	st.SetCompress(true)
	pad := strings.Repeat("y", 2048)
	for i := 0; i < 10; i++ {
		if _, err := st.Apply("c", docName(i), map[string]interface{}{"n": i, "pad": pad}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Delete("c", docName(9)); err != nil {
		t.Fatal(err)
	}
	st.Close()

	// Reopen WITHOUT compression: replay must inflate every record.
	st2, err := Open(dir)
	if err != nil {
		t.Fatalf("replay of a compressed log: %v", err)
	}
	defer st2.Close()
	if st2.Compressing() {
		t.Fatal("flag should default to off")
	}
	for i := 0; i < 9; i++ {
		d, ok := st2.Get("c", docName(i))
		if !ok {
			t.Fatalf("record %d lost across compression replay", i)
		}
		// After a restart the number came through JSON, so it is a
		// float64; in-process it is an int. Both are "record i".
		switch v := d.Fields["n"].(type) {
		case int:
			if v != i {
				t.Fatalf("record %d corrupted: %#v", i, v)
			}
		case float64:
			if int(v) != i {
				t.Fatalf("record %d corrupted: %#v", i, v)
			}
		default:
			t.Fatalf("record %d has unexpected type %#v", i, d.Fields["n"])
		}
	}
	if d, ok := st2.Get("c", docName(9)); ok && !d.Deleted {
		t.Error("tombstone lost across compression replay")
	}
}

// TestSmallRecordsStayRaw: framing a 100-byte record only makes it
// bigger, and a bigger record is a slower replay.
func TestSmallRecordsStayRaw(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	st.SetCompress(true)
	if _, err := st.Apply("c", "tiny", map[string]interface{}{"n": 1}); err != nil {
		t.Fatal(err)
	}
	st.Close()
	line := strings.SplitN(logBytes(t, dir), "\n", 2)[0]
	payload, ok := verifyRecord(line)
	if !ok {
		t.Fatalf("record failed checksum: %.60s", line)
	}
	if strings.HasPrefix(payload, compressPrefix) {
		t.Errorf("tiny record was compressed anyway: %.60s", payload)
	}
}

// TestCompressPlusEncrypt: compress-then-encrypt must survive a full
// round trip (the CRC covers the ciphertext, the flag lives inside it).
func TestCompressPlusEncrypt(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenWithKey(dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	st.SetCompress(true)
	pad := strings.Repeat("secret secret secret ", 100)
	if _, err := st.Apply("c", "s", map[string]interface{}{"v": pad}); err != nil {
		t.Fatal(err)
	}
	st.Close()

	raw := logBytes(t, dir)
	if strings.Contains(raw, "secret") {
		t.Fatal("plaintext leaked into a compressed+encrypted log")
	}

	st2, err := OpenWithKey(dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	d, ok := st2.Get("c", "s")
	if !ok || d.Fields["v"] != pad {
		t.Fatalf("round trip lost data (ok=%v)", ok)
	}
}
