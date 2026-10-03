// Package backup runs scheduled, verified, retained backups to a cold
// target — the RPO half of "production grade". microctl does the same
// work by hand; this does it on a timer, in-process, and bounded.
//
// In-process matters: the node's own store is the only handle that can
// snapshot it consistently while it is writing. Opening the data
// directory from a second process (what `microctl backup` does) is for
// cold or emergency use, not for a scheduler running beside a live
// writer.
package backup

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"microdb/internal/store"
	"microdb/internal/tier"
)

// DefaultPrefix namespaces scheduled backups on the target, so
// retention prunes scheduled archives and nothing else.
const DefaultPrefix = "backup"

// Config describes one scheduled backup destination.
type Config struct {
	// St is the LIVE store to snapshot (read under its lock, so the
	// snapshot is consistent with concurrent writes).
	St *store.Store
	// Target is where archives go (dir:// or s3://).
	Target tier.Target
	// Keep is how many archives to retain on the target (<=0 = 10).
	Keep int
	// Prefix namespaces the objects (default DefaultPrefix).
	Prefix string
	// Now is injectable for tests that need distinct timestamps.
	Now func() time.Time
}

func (c *Config) normalize() {
	if c.Keep <= 0 {
		c.Keep = 10
	}
	if c.Prefix == "" {
		c.Prefix = DefaultPrefix
	}
	if c.Now == nil {
		c.Now = time.Now
	}
}

// RunOnce writes one backup and its manifest to the target, then
// enforces retention. It returns the object key and the manifest.
//
// The archive goes through a temp file so the upload always has a
// known Content-Length (chunked uploads are not portable across S3
// implementations) and so a crash mid-write never publishes a partial
// object under the final name.
func RunOnce(cfg Config) (string, store.BackupManifest, error) {
	cfg.normalize()
	if cfg.St == nil || cfg.Target == nil {
		return "", store.BackupManifest{}, fmt.Errorf("backup: store and target are required")
	}
	tmp, err := os.CreateTemp("", "microdb-scheduled-*.jsonl")
	if err != nil {
		return "", store.BackupManifest{}, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	manifest, err := cfg.St.BackupWithManifest(tmp)
	if err != nil {
		tmp.Close()
		return "", store.BackupManifest{}, err
	}
	if err := tmp.Close(); err != nil {
		return "", store.BackupManifest{}, err
	}
	if fi, err := os.Stat(tmpName); err == nil {
		manifest.Bytes = fi.Size()
	}

	base := fmt.Sprintf("%s-%d.jsonl", cfg.Prefix, manifest.CreatedUnixM)
	f, err := os.Open(tmpName)
	if err != nil {
		return "", store.BackupManifest{}, err
	}
	err = cfg.Target.Put(base, f, manifest.Bytes)
	f.Close()
	if err != nil {
		return "", store.BackupManifest{}, err
	}
	mb, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return base, manifest, err
	}
	if err := cfg.Target.Put(base+".manifest.json", strings.NewReader(string(mb)+"\n"), int64(len(mb)+1)); err != nil {
		return base, manifest, err
	}
	if _, err := Prune(cfg.Target, cfg.Prefix, cfg.Keep); err != nil {
		// A retention failure must not fail the backup that succeeded:
		// report it and let the next tick retry the prune.
		return base, manifest, fmt.Errorf("backup written but prune failed: %w", err)
	}
	return base, manifest, nil
}

// Prune deletes the oldest archives beyond keep, each together with
// its manifest. Returns how many archives were removed.
func Prune(t tier.Target, prefix string, keep int) (int, error) {
	if keep <= 0 {
		keep = 10
	}
	keys, err := t.List()
	if err != nil {
		return 0, err
	}
	var archives []string
	for _, k := range keys {
		if strings.HasPrefix(k, prefix+"-") && strings.HasSuffix(k, ".jsonl") {
			archives = append(archives, k)
		}
	}
	// Keys embed unix millis, so lexicographic order is chronological.
	sort.Strings(archives)
	removed := 0
	for len(archives) > keep {
		k := archives[0]
		archives = archives[1:]
		if err := t.Delete(k); err != nil {
			return removed, fmt.Errorf("prune %s: %w", k, err)
		}
		// A manifest without its archive is a trap for whoever verifies
		// next: remove it in the same pass.
		_ = t.Delete(k + ".manifest.json")
		removed++
	}
	return removed, nil
}

// Run drives RunOnce on an interval until stop is closed. Errors are
// reported through logf and retried on the next tick — a backup that
// fails once must not stop the ones after it.
func Run(stop <-chan struct{}, every time.Duration, cfg Config, logf func(format string, args ...interface{})) {
	if every <= 0 || logf == nil {
		return
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			key, m, err := RunOnce(cfg)
			if err != nil {
				logf("backup failed (will retry next tick): %v", err)
				continue
			}
			logf("backup %s: %d docs, %d bytes -> %s", key, m.Docs, m.Bytes, cfg.Target.Name())
		}
	}
}
