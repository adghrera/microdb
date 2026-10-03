package tier

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOpenParsesTargets(t *testing.T) {
	dir := t.TempDir()
	tgt, err := Open("dir://" + filepath.ToSlash(dir))
	if err != nil {
		t.Fatalf("dir target: %v", err)
	}
	if _, ok := tgt.(*DirTarget); !ok {
		t.Fatalf("expected *DirTarget, got %T", tgt)
	}

	if _, err := Open("ftp://nope"); err == nil {
		t.Error("unsupported scheme must be rejected")
	}
	if _, err := Open("s3://bucket/p"); err == nil {
		t.Error("s3 without credentials must be rejected at construction, not at first archive")
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	s3, err := Open("s3://bucket/prefix?region=eu-west-1")
	if err != nil {
		t.Fatalf("s3 target: %v", err)
	}
	if !strings.Contains(s3.Name(), "eu-west-1") {
		t.Errorf("region not carried into the target name: %s", s3.Name())
	}
}

func TestDirTargetRoundTrip(t *testing.T) {
	tgt, err := NewDirTarget(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("history-1\nhistory-2\n")
	if err := tgt.Put("archive-1.jsonl", bytes.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatal(err)
	}
	rc, err := tgt.Get("archive-1.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, payload) {
		t.Fatalf("round trip changed the bytes: %q", got)
	}
	keys, err := tgt.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "archive-1.jsonl" {
		t.Fatalf("list = %v", keys)
	}
	if _, err := tgt.Get("missing.jsonl"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing key error = %v, want ErrNotFound", err)
	}
}

// A key must never be able to write outside the target root: archives
// arrive from config files and CLI flags.
func TestDirTargetRejectsEscapingKeys(t *testing.T) {
	root := t.TempDir()
	tgt, err := NewDirTarget(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := tgt.Put("../../evil.jsonl", strings.NewReader("x"), 1); err == nil {
		t.Fatal("expected a path-escape rejection")
	}
	escaped, err := filepath.Abs(filepath.Join(root, "..", "evil.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(escaped); err == nil {
		t.Fatalf("escape wrote outside the root: %s", escaped)
	}
}

func TestArchiverNamesAndFetches(t *testing.T) {
	tgt, err := NewDirTarget(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	a := &Archiver{Target: tgt, Prefix: "log", Now: func() time.Time {
		return time.UnixMilli(1700000000123)
	}}
	seg, err := a.Put(strings.NewReader("abc"), 3)
	if err != nil {
		t.Fatal(err)
	}
	if seg.Key != "log-1700000000123.jsonl" {
		t.Errorf("key = %q, want timestamped name", seg.Key)
	}
	var buf bytes.Buffer
	if err := a.Fetch(seg.Key, &buf); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "abc" {
		t.Errorf("fetched %q", buf.String())
	}
	if err := a.Fetch("nope.jsonl", &buf); !errors.Is(err, ErrNotFound) {
		t.Errorf("fetch missing = %v, want ErrNotFound", err)
	}
}
