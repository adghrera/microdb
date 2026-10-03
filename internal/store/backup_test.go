package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func backupFixture(t *testing.T, dir string, n int) *Store {
	t.Helper()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if _, err := st.Apply("c", docName(i), map[string]interface{}{"n": i}); err != nil {
			t.Fatal(err)
		}
	}
	return st
}

func TestBackupManifestVerifies(t *testing.T) {
	dir := t.TempDir()
	st := backupFixture(t, filepath.Join(dir, "db"), 25)
	defer st.Close()

	backup := filepath.Join(dir, "snap.jsonl")
	f, err := os.Create(backup)
	if err != nil {
		t.Fatal(err)
	}
	m, err := st.BackupWithManifest(f)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	fi, _ := os.Stat(backup)
	m.Bytes = fi.Size()
	if err := m.WriteManifestFile(backup); err != nil {
		t.Fatal(err)
	}

	rep, err := VerifyBackup(backup)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK() {
		t.Fatalf("fresh backup did not verify: %v", rep.Problems)
	}
	if rep.Docs != 25 || m.Docs != 25 {
		t.Errorf("docs = %d (manifest %d), want 25", rep.Docs, m.Docs)
	}
	if rep.SHA256 != m.SHA256 {
		t.Errorf("digest mismatch: %s vs %s", rep.SHA256, m.SHA256)
	}
}

// TestBackupsAreDeterministic: two backups of identical state must be
// byte-identical, otherwise the digest means nothing.
func TestBackupsAreDeterministic(t *testing.T) {
	dir := t.TempDir()
	st := backupFixture(t, filepath.Join(dir, "db"), 30)
	defer st.Close()

	var a, b strings.Builder
	if _, err := st.BackupWithManifest(&a); err != nil {
		t.Fatal(err)
	}
	if _, err := st.BackupWithManifest(&b); err != nil {
		t.Fatal(err)
	}
	if a.String() != b.String() {
		t.Fatal("backup bytes differ between runs of the same state")
	}
}

func TestVerifyBackupDetectsTampering(t *testing.T) {
	dir := t.TempDir()
	st := backupFixture(t, filepath.Join(dir, "db"), 10)
	defer st.Close()
	backup := filepath.Join(dir, "snap.jsonl")
	f, _ := os.Create(backup)
	m, err := st.BackupWithManifest(f)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	fi, _ := os.Stat(backup)
	m.Bytes = fi.Size()
	m.WriteManifestFile(backup)

	// Someone edits one document after the fact.
	raw, _ := os.ReadFile(backup)
	tampered := strings.Replace(string(raw), `"n":3`, `"n":9`, 1)
	if tampered == string(raw) {
		t.Fatal("fixture did not actually change")
	}
	os.WriteFile(backup, []byte(tampered), 0o644)

	rep, err := VerifyBackup(backup)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK() {
		t.Fatal("tampered backup verified OK")
	}
	joined := strings.Join(rep.Problems, "; ")
	if !strings.Contains(joined, "digest mismatch") {
		t.Errorf("problems = %v, want a digest mismatch", rep.Problems)
	}
}

func TestVerifyBackupMissingManifestFails(t *testing.T) {
	dir := t.TempDir()
	st := backupFixture(t, filepath.Join(dir, "db"), 5)
	defer st.Close()
	backup := filepath.Join(dir, "snap.jsonl")
	f, _ := os.Create(backup)
	if _, err := st.BackupWithManifest(f); err != nil {
		t.Fatal(err)
	}
	f.Close()

	rep, err := VerifyBackup(backup)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK() {
		t.Fatal("a backup with no manifest must not verify OK")
	}
	if !strings.Contains(strings.Join(rep.Problems, "; "), "manifest missing") {
		t.Errorf("problems = %v, want a missing-manifest complaint", rep.Problems)
	}
}

func TestVerifyBackupDetectsTruncation(t *testing.T) {
	dir := t.TempDir()
	st := backupFixture(t, filepath.Join(dir, "db"), 10)
	defer st.Close()
	backup := filepath.Join(dir, "snap.jsonl")
	f, _ := os.Create(backup)
	m, err := st.BackupWithManifest(f)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	fi, _ := os.Stat(backup)
	m.Bytes = fi.Size()
	m.WriteManifestFile(backup)

	// Chop the last document in half.
	raw, _ := os.ReadFile(backup)
	os.WriteFile(backup, raw[:len(raw)-40], 0o644)

	rep, err := VerifyBackup(backup)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK() {
		t.Fatal("truncated backup verified OK")
	}
	if len(rep.BadLines) == 0 {
		t.Error("expected the torn line to be reported")
	}
	joined := strings.Join(rep.Problems, "; ")
	if !strings.Contains(joined, "do not parse") {
		t.Errorf("problems = %v, want a parse complaint", rep.Problems)
	}
}
