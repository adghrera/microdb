package cluster

import (
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"

	"microdb/internal/metrics"
)

// certCache serves the node's own leaf certificate and re-reads the
// files whenever they change.
//
// The check is one stat() per TLS handshake rather than a background
// timer: handshakes are rare, stat is ~microseconds, and a reload that
// happens on the next connection is exactly what an operator rotating
// files on disk expects — no signal, no restart, no window where the
// old cert is served after the new one was written.
//
// A file that fails to load (a writer that has not finished, a
// mismatched key) does NOT break serving: the previous certificate
// stays in use and the failure is logged, so a botched rotation degrades
// to "still the old cert" instead of "no TLS". The next handshake
// retries.
type certCache struct {
	certFile string
	keyFile  string

	mu    sync.Mutex
	cert  *tls.Certificate
	stamp string
	errs  int64
}

// stamp identifies the current file contents cheaply: size + mtime of
// both files. (Not a content hash: this runs on every handshake.)
func (c *certCache) stampFiles() (string, error) {
	cs, err := os.Stat(c.certFile)
	if err != nil {
		return "", fmt.Errorf("stat %s: %w", c.certFile, err)
	}
	ks, err := os.Stat(c.keyFile)
	if err != nil {
		return "", fmt.Errorf("stat %s: %w", c.keyFile, err)
	}
	return fmt.Sprintf("%d/%d/%d/%d", cs.Size(), cs.ModTime().UnixNano(),
		ks.Size(), ks.ModTime().UnixNano()), nil
}

// get returns the current certificate, re-reading the files if they
// changed since the last handshake.
func (c *certCache) get() (*tls.Certificate, error) {
	stamp, err := c.stampFiles()
	if err != nil {
		c.mu.Lock()
		cert := c.cert // keep serving what we have if stat flakes
		c.mu.Unlock()
		if cert != nil {
			return cert, nil
		}
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cert != nil && c.stamp == stamp {
		return c.cert, nil
	}
	cert, err := tls.LoadX509KeyPair(c.certFile, c.keyFile)
	if err != nil {
		c.errs++
		atomic.AddInt64(metrics.Default.Counter("microdb_tls_cert_reload_errors_total",
			"Certificate reloads that failed (previous cert kept)"), 1)
		if c.cert != nil {
			log.Printf("tls: certificate reload failed (%v) — continuing with the previous certificate", err)
			return c.cert, nil
		}
		return nil, fmt.Errorf("load %s: %w", c.certFile, err)
	}
	first := c.cert == nil
	c.cert = &cert
	c.stamp = stamp
	if !first {
		atomic.AddInt64(metrics.Default.Counter("microdb_tls_cert_reloads_total",
			"Certificate reloads picked up from disk"), 1)
		log.Printf("tls: loaded renewed certificate %s (next handshake serves it)", c.certFile)
	}
	return c.cert, nil
}

// reloadStats reports (successful reloads, failed reloads) for tests
// and diagnostics.
func (c *certCache) reloadStats() (int64, int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	errs := c.errs
	good := atomic.LoadInt64(metrics.Default.Counter("microdb_tls_cert_reloads_total", ""))
	return good, errs
}

// --- TLSOptions integration -----------------------------------------

// certs lazily builds the cache for this options value. TLSOptions is
// configured once at startup, before any handshake, so the lazy
// construction needs no lock of its own.
func (o *TLSOptions) certs() *certCache {
	if o.cache == nil {
		o.cache = &certCache{certFile: o.CertFile, keyFile: o.KeyFile}
	}
	return o.cache
}

// loadEager validates the pair once at startup so a broken certificate
// fails the process start rather than the first connection.
func (o *TLSOptions) loadEager() (*tls.Certificate, error) {
	return o.certs().get()
}
