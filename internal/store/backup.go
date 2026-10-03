package store

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

// sortDocs puts documents in a stable, deterministic order so two
// backups of identical state are byte-identical — which is what makes
// a digest meaningful.
func sortDocs(docs []*Doc) {
	sort.Slice(docs, func(i, j int) bool {
		if docs[i].Collection != docs[j].Collection {
			return docs[i].Collection < docs[j].Collection
		}
		return docs[i].ID < docs[j].ID
	})
}

// BackupFormat identifies a backup file's layout so a future format
// change fails loudly instead of restoring the wrong bytes.
const BackupFormat = "microdb-backup-1"

// BackupManifest is the receipt for a backup: how many documents it
// holds, how big it is, and a digest of its exact bytes. Verification
// compares the file against this — an untested backup is not a backup.
type BackupManifest struct {
	Format       string `json:"format"`
	CreatedUnixM int64  `json:"created_unix_ms"`
	Docs         int    `json:"docs"`
	Bytes        int64  `json:"bytes"`
	SHA256       string `json:"sha256"`
}

// BackupWithManifest writes the backup to w and returns the manifest
// describing exactly what was written (the digest covers the bytes that
// went out, so it can be checked later against the file on disk).
func (s *Store) BackupWithManifest(w io.Writer) (BackupManifest, error) {
	h := sha256.New()
	n, err := s.backupCounted(io.MultiWriter(w, h))
	if err != nil {
		return BackupManifest{}, err
	}
	m := BackupManifest{
		Format:       BackupFormat,
		CreatedUnixM: time.Now().UnixMilli(),
		Docs:         n,
		SHA256:       hex.EncodeToString(h.Sum(nil)),
	}
	return m, nil
}

// backupCounted is Backup, but it also reports how many documents it
// wrote so the manifest can state it.
func (s *Store) backupCounted(w io.Writer) (int, error) {
	s.mu.RLock()
	docs := make([]*Doc, 0, s.nDocs.Load())
	s.rangeDocs(func(_ string, d *Doc) bool {
		docs = append(docs, d)
		return true
	})
	s.mu.RUnlock()
	sortDocs(docs)

	bw := bufio.NewWriterSize(w, 1<<20)
	enc := json.NewEncoder(bw)
	for _, d := range docs {
		if err := enc.Encode(d); err != nil {
			return 0, err
		}
	}
	if err := bw.Flush(); err != nil {
		return 0, err
	}
	return len(docs), nil
}

// WriteManifestFile writes m next to a backup file (same path with a
// .manifest.json suffix).
func (m BackupManifest) WriteManifestFile(backupPath string) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(ManifestPath(backupPath), append(b, '\n'), 0o644)
}

// ManifestPath is where a backup's manifest lives.
func ManifestPath(backupPath string) string { return backupPath + ".manifest.json" }

// BackupReport is the result of checking a backup file.
type BackupReport struct {
	Path     string          `json:"path"`
	Docs     int             `json:"docs"`
	Bytes    int64           `json:"bytes"`
	SHA256   string          `json:"sha256"`
	BadLines []int           `json:"bad_lines,omitempty"`
	Manifest *BackupManifest `json:"manifest,omitempty"`
	Problems []string        `json:"problems,omitempty"`
}

// OK reports whether the backup verified: every line parses as a
// document, and a manifest exists whose counts and digest match.
func (r BackupReport) OK() bool { return len(r.Problems) == 0 && r.Manifest != nil }

// VerifyBackup checks a backup file and, when present, its manifest.
//
// What it proves: the file is complete JSONL (no truncated line), its
// digest is what the manifest recorded, and the document count matches.
// What it cannot prove: that the documents are the ones you meant to
// save — that needs a restore, which is why the report is also usable
// against a scratch directory.
func VerifyBackup(path string) (BackupReport, error) {
	rep := BackupReport{Path: path}
	f, err := os.Open(path)
	if err != nil {
		return rep, err
	}
	defer f.Close()
	if fi, err := f.Stat(); err == nil {
		rep.Bytes = fi.Size()
	}

	h := sha256.New()
	br := bufio.NewReaderSize(io.TeeReader(f, h), 1<<20)
	lineNo := 0
	for {
		line, err := br.ReadString('\n')
		if len(line) > 0 {
			lineNo++
			trimmed := strings.TrimSpace(line)
			if trimmed == "" {
				continue
			}
			var d Doc
			if json.Unmarshal([]byte(trimmed), &d) != nil || d.ID == "" {
				rep.BadLines = append(rep.BadLines, lineNo)
			} else {
				rep.Docs++
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return rep, err
		}
	}
	rep.SHA256 = hex.EncodeToString(h.Sum(nil))

	if len(rep.BadLines) > 0 {
		rep.Problems = append(rep.Problems,
			fmt.Sprintf("%d line(s) do not parse as documents: %v", len(rep.BadLines), firstN(rep.BadLines, 5)))
	}

	mb, err := os.ReadFile(ManifestPath(path))
	if err != nil {
		rep.Problems = append(rep.Problems, "manifest missing — nothing to check the file against")
		return rep, nil
	}
	var m BackupManifest
	if err := json.Unmarshal(mb, &m); err != nil {
		rep.Problems = append(rep.Problems, "manifest is not valid JSON")
		return rep, nil
	}
	rep.Manifest = &m
	if m.Format != BackupFormat {
		rep.Problems = append(rep.Problems, fmt.Sprintf("unknown backup format %q", m.Format))
	}
	if m.SHA256 != rep.SHA256 {
		rep.Problems = append(rep.Problems,
			fmt.Sprintf("digest mismatch: manifest %s, file %s — the file changed after it was written", short(m.SHA256), short(rep.SHA256)))
	}
	if int64(m.Docs) != int64(rep.Docs) {
		rep.Problems = append(rep.Problems,
			fmt.Sprintf("document count mismatch: manifest %d, file %d", m.Docs, rep.Docs))
	}
	if m.Bytes != 0 && m.Bytes != rep.Bytes {
		rep.Problems = append(rep.Problems,
			fmt.Sprintf("size mismatch: manifest %d bytes, file %d", m.Bytes, rep.Bytes))
	}
	return rep, nil
}

func short(h string) string {
	if len(h) > 12 {
		return h[:12] + "…"
	}
	return h
}

func firstN(xs []int, n int) []int {
	if len(xs) <= n {
		return xs
	}
	return xs[:n]
}
