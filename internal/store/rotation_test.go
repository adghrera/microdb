package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testKeyB is the key we rotate TO.
const testKeyB = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"

func writeWith(t *testing.T, dir, keyHex string, prev []string, id string, v string) {
	t.Helper()
	// Every key must be installed BEFORE replay: the log already holds
	// records from the previous era.
	st, err := OpenWithKeyRing(dir, keyHex, prev)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Apply("c", id, map[string]interface{}{"v": v}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestKeyRotationKeepsEveryEraReadable is the rotation contract: new
// records go under the new key, old records still open because the old
// key stays configured for decryption, and each record says which key
// it needs.
func TestKeyRotationKeepsEveryEraReadable(t *testing.T) {
	dir := t.TempDir()

	// Era A.
	writeWith(t, dir, testKey, nil, "eraA", "a")
	// Rotation: primary becomes B, A is kept for decryption only.
	writeWith(t, dir, testKeyB, []string{testKey}, "eraB", "b")
	// Second rotation: C primary, B and A still configured for
	// decryption — dropping a key is a separate, deliberate act.
	writeWith(t, dir, "1111111111111111111111111111111111111111111111111111111111111111",
		[]string{testKeyB, testKey}, "eraC", "c")

	// Reopen with the full ring: every era readable.
	st, err := OpenWithKeyRing(dir, "1111111111111111111111111111111111111111111111111111111111111111", []string{testKeyB, testKey})
	if err != nil {
		t.Fatalf("open with full ring: %v", err)
	}
	for _, want := range []struct{ id, v string }{{"eraA", "a"}, {"eraB", "b"}, {"eraC", "c"}} {
		d, ok := st.Get("c", want.id)
		if !ok || d.Fields["v"] != want.v {
			t.Errorf("%s unreadable: ok=%v doc=%v", want.id, ok, d)
		}
	}
	ids := st.PreviousKeyIDs()
	if len(ids) != 2 {
		t.Fatalf("PreviousKeyIDs = %v, want both retired key ids", ids)
	}
	want := map[string]bool{keyIDOf(mustKey(t, testKey)): true, keyIDOf(mustKey(t, testKeyB)): true}
	for _, id := range ids {
		if !want[id] {
			t.Errorf("unexpected retired key id %q", id)
		}
	}
	st.Close()

	raw, err := os.ReadFile(filepath.Join(dir, "data.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	// Every era's records are stamped with the key they need: three
	// records, three distinct key ids, each resolvable.
	kids := map[string]bool{}
	for _, ln := range strings.Split(string(raw), "\n") {
		if ln == "" {
			continue
		}
		payload, ok := verifyRecord(ln)
		if !ok || !strings.HasPrefix(payload, encPrefixV2) {
			t.Fatalf("record not in ENC2 form: %.60s", ln)
		}
		kids[strings.SplitN(payload[len(encPrefixV2):], ":", 2)[0]] = true
	}
	wantKids := map[string]bool{
		keyIDOf(mustKey(t, testKey)):  true,
		keyIDOf(mustKey(t, testKeyB)): true,
		keyIDOf(mustKey(t, "1111111111111111111111111111111111111111111111111111111111111111")): true,
	}
	if len(kids) != len(wantKids) {
		t.Fatalf("key ids in the log = %v, want %v", kids, wantKids)
	}
	for k := range wantKids {
		if !kids[k] {
			t.Errorf("no record stamped with key id %q (got %v)", k, kids)
		}
	}

	// Dropping a still-needed key must fail loudly and name it.
	_, err = OpenWithKey(dir, testKeyB)
	if err == nil {
		t.Fatal("opening without the era-A key should fail")
	}
	if !strings.Contains(err.Error(), "key id") {
		t.Errorf("error should name the missing key id, got: %v", err)
	}
}

// TestLegacyEnc1RecordsStillOpen: a log written before key ids existed
// (ENC1:<b64>) must still decrypt by trying the configured keys —
// otherwise rotation strands every record written by an older binary.
func TestLegacyEnc1RecordsStillOpen(t *testing.T) {
	dir := t.TempDir()
	keyBytes := mustKey(t, testKey)
	doc := Doc{ID: "legacy", Collection: "c", Ver: 1, TS: 1,
		Fields: map[string]interface{}{"v": "from-enc1-era"}}
	plain, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	body, err := encryptRecord(keyBytes, plain)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(body), encPrefix) {
		t.Fatalf("fixture is not an ENC1 record: %.40s", body)
	}
	line := crcPrefix + checksumHex(body) + ":" + string(body) + "\n"
	if err := os.WriteFile(filepath.Join(dir, "data.jsonl"), []byte(line), 0o644); err != nil {
		t.Fatal(err)
	}

	st, err := OpenWithKey(dir, testKey)
	if err != nil {
		t.Fatalf("legacy record did not open: %v", err)
	}
	defer st.Close()
	d, ok := st.Get("c", "legacy")
	if !ok || d.Fields["v"] != "from-enc1-era" {
		t.Fatalf("legacy record lost: ok=%v doc=%v", ok, d)
	}
}
