// Package tier stores closed segments of history somewhere other than
// the node's own disk — the cold half of the storage tier.
//
// microdb keeps the working set resident and the live log local; what
// grows without bound is *history*: the raw log that PITR and audits
// need. Tiering moves each archived copy to a target and leaves only a
// pointer behind, so local disk tracks the live dataset while history
// tracks object storage.
//
// Two targets ship:
//
//   - DirTarget: a directory (a NAS mount, a synced bucket mount, or
//     another disk). Fully exercised by the integration tests.
//   - S3Target: S3-compatible object storage spoken over net/http with
//     AWS Signature V4 — no SDK, no dependency. Verified against an
//     in-process S3 stand-in that validates the signature, canonical
//     request and payload (a live bucket was not available here; see
//     the FEATURES note).
package tier

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// ErrNotFound is returned when a key does not exist on the target.
var ErrNotFound = errors.New("tier: object not found")

// Target is where cold segments go.
type Target interface {
	// Name identifies the target in logs and microctl output.
	Name() string
	// Put stores the whole reader under key (atomic from a reader's
	// point of view: a failed put leaves nothing readable).
	Put(key string, r io.Reader, size int64) error
	// Get returns the object, or ErrNotFound.
	Get(key string) (io.ReadCloser, error)
	// List returns keys in a stable order, or an empty slice.
	List() ([]string, error)
	// Delete removes one key. Deleting a missing key is not an error:
	// retention is a goal ("at most Keep remain"), not a transaction.
	Delete(key string) error
}

// Open parses a target URI:
//
//	dir:///path/to/dir            -> directory target
//	s3://bucket/prefix            -> S3 (region/credentials from env)
//	s3://bucket/prefix?region=eu-west-1&endpoint=https://minio:9000
func Open(uri string) (Target, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("tier: bad target %q: %w", uri, err)
	}
	switch strings.ToLower(u.Scheme) {
	case "", "dir", "file":
		p := u.Path
		// dir:///srv/tier        -> /srv/tier
		// dir://C:/tier          -> C:/tier   (Windows drive)
		// dir://server/share     -> //server/share (UNC)
		if u.Host != "" {
			if strings.HasSuffix(u.Host, ":") {
				p = u.Host + u.Path
			} else {
				p = "/" + u.Host + u.Path
			}
		}
		if p == "" {
			p = u.Opaque
		}
		if p == "" {
			return nil, fmt.Errorf("tier: dir target needs a path (dir:///srv/tier)")
		}
		return NewDirTarget(p)
	case "s3":
		if u.Host == "" {
			return nil, fmt.Errorf("tier: s3 target needs a bucket (s3://bucket/prefix)")
		}
		q := u.Query()
		return NewS3Target(S3Options{
			Bucket:    u.Host,
			Prefix:    strings.Trim(u.Path, "/"),
			Region:    firstNonEmpty(q.Get("region"), os.Getenv("AWS_REGION"), os.Getenv("AWS_DEFAULT_REGION"), "us-east-1"),
			Endpoint:  firstNonEmpty(q.Get("endpoint"), os.Getenv("MICRODB_S3_ENDPOINT")),
			AccessKey: firstNonEmpty(q.Get("access_key"), os.Getenv("AWS_ACCESS_KEY_ID")),
			SecretKey: firstNonEmpty(q.Get("secret_key"), os.Getenv("AWS_SECRET_ACCESS_KEY")),
		})
	default:
		return nil, fmt.Errorf("tier: unsupported target scheme %q", u.Scheme)
	}
}

func firstNonEmpty(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}

// --- directory target ----------------------------------------------

// DirTarget stores objects as files under a root directory.
type DirTarget struct {
	root string
}

// NewDirTarget creates the directory if needed.
func NewDirTarget(root string) (*DirTarget, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("tier: %w", err)
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	return &DirTarget{root: abs}, nil
}

func (d *DirTarget) Name() string { return "dir://" + d.root }

func (d *DirTarget) pathFor(key string) (string, error) {
	// Keys are ours (archive-<ts>.jsonl) but never trust one blindly:
	// a traversal segment is rejected outright rather than silently
	// cleaned, so a bad key fails loudly instead of landing somewhere
	// surprising.
	for _, seg := range strings.FieldsFunc(key, func(r rune) bool { return strings.ContainsRune("/\\", r) }) {
		if seg == ".." {
			return "", fmt.Errorf("tier: key %q escapes the target root", key)
		}
	}
	clean := filepath.Clean("/" + filepath.FromSlash(key))
	full := filepath.Join(d.root, clean)
	if !strings.HasPrefix(full, d.root) {
		return "", fmt.Errorf("tier: key %q escapes the target root", key)
	}
	return full, nil
}

// Put writes to a temporary file first and renames it into place, so a
// reader never sees a half-written object.
func (d *DirTarget) Put(key string, r io.Reader, size int64) error {
	dst, err := d.pathFor(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	tmp := dst + ".part"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dst)
}

func (d *DirTarget) Get(key string) (io.ReadCloser, error) {
	p, err := d.pathFor(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if os.IsNotExist(err) {
		return nil, ErrNotFound
	}
	return f, err
}

// Delete removes one archived object (and is a no-op if absent).
func (d *DirTarget) Delete(key string) error {
	p, err := d.pathFor(key)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (d *DirTarget) List() ([]string, error) {
	var out []string
	err := filepath.Walk(d.root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || strings.HasSuffix(p, ".part") {
			return nil
		}
		rel, err := filepath.Rel(d.root, p)
		if err != nil {
			return err
		}
		out = append(out, filepath.ToSlash(rel))
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

// --- archiving -------------------------------------------------------

// Segment is one archived object's identity.
type Segment struct {
	Key  string `json:"key"`
	Size int64  `json:"bytes"`
	Time int64  `json:"unix_ms"`
}

// Archiver writes raw log history to a target under a timestamped key
// and keeps a manifest so an operator (and microctl) can list what is
// cold without walking the bucket.
type Archiver struct {
	Target Target
	// Prefix names the archived objects (default "archive").
	Prefix string
	// Now is injectable for tests.
	Now func() time.Time
}

// Put stores r as <prefix>-<unixmilli>-<n>.jsonl and returns the
// segment identity.
func (a *Archiver) Put(r io.Reader, size int64) (Segment, error) {
	prefix := a.Prefix
	if prefix == "" {
		prefix = "archive"
	}
	now := time.Now
	if a.Now != nil {
		now = a.Now
	}
	key := fmt.Sprintf("%s-%d.jsonl", prefix, now().UnixMilli())
	if err := a.Target.Put(key, r, size); err != nil {
		return Segment{}, err
	}
	return Segment{Key: key, Size: size, Time: now().UnixMilli()}, nil
}

// Fetch writes the object to w.
func (a *Archiver) Fetch(key string, w io.Writer) error {
	rc, err := a.Target.Get(path.Clean(key))
	if err != nil {
		return err
	}
	defer rc.Close()
	_, err = io.Copy(w, rc)
	return err
}
