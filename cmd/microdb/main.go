// microdb — a tiny distributed schema-less document store.
//
// Usage:
//
//	microdb --addr :8001 --dir ./data1 [--join http://127.0.0.1:8002]
//
// API:
//
//	PUT    /api/collections/{col}/docs/{id}   {"any":"json"}
//	GET    /api/collections/{col}/docs/{id}
//	DELETE /api/collections/{col}/docs/{id}
//	GET    /api/collections/{col}/docs?filter={"age":{"$gt":30}}
//	GET    /api/cluster
//	GET    /health
package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"microdb/internal/api"
	"microdb/internal/cluster"
	"microdb/internal/store"
	"microdb/internal/tenants"
)

func main() {
	addr := flag.String("addr", ":8001", "listen address")
	dir := flag.String("dir", "./data", "data directory")
	join := flag.String("join", "", "comma-separated seed nodes, e.g. http://127.0.0.1:8002")
	fsync := flag.Bool("fsync", false, "fsync every write to disk (durable, slower)")
	tlsCert := flag.String("tls-cert", "", "PEM cert for TLS (enables https)")
	tlsKey := flag.String("tls-key", "", "PEM key for TLS")
	tlsCA := flag.String("tls-ca", "", "PEM CA bundle to verify peer certs (mutual TLS)")
	authToken := flag.String("auth-token", "", "require this bearer token on /api/* (empty = open)")
	rf := flag.Int("rf", 3, "replication factor (ring owners per key)")
	archiveDir := flag.String("archive-dir", "", "continuously archive the raw commit log here (PITR)")
	archiveInterval := flag.Duration("archive-interval", 60*time.Second, "how often to write a raw-log archive (with --archive-dir)")
	maxInflight := flag.Int64("max-inflight", 0, "shed load with 429 above this many concurrent requests (0 = unlimited)")
	traceSlowMs := flag.Int64("trace-slow-ms", 500, "log requests slower than this with their trace id")
	encKey := flag.String("encryption-key", "", "64-hex-char (32-byte) AES-256-GCM key for encryption at rest")
	clusterName := flag.String("cluster-name", "", "cluster identity guard: nodes only join peers with the same name")
	jsonLog := flag.Bool("json-log", false, "emit structured JSON logs")
	idemTTL := flag.Duration("idempotency-ttl", 10*time.Minute, "dedupe window for Idempotency-Key retries (0 disables)")
	durableFeed := flag.Bool("durable-feed", false, "persist the watch change feed to disk (cursors survive restart, 24h retention)")
	tenantsFile := flag.String("tenants", "", "JSON file with tenant definitions (tokens, rate limits, quotas); enables multi-tenancy")
	flag.Parse()

	if *jsonLog {
		log.SetFlags(0)
		log.SetOutput(&jsonLogWriter{w: os.Stderr})
	}

	st, err := store.OpenWithKey(*dir, *encKey)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	if *durableFeed {
		if err := st.EnableDurableFeed(); err != nil {
			log.Fatalf("enable durable feed: %v", err)
		}
	}
	st.SetFsync(*fsync)
	defer st.Close()

	var tlsOpts *cluster.TLSOptions
	if *tlsCert != "" {
		if *tlsKey == "" || *tlsCA == "" {
			log.Fatal("--tls-cert requires --tls-key and --tls-ca")
		}
		tlsOpts = &cluster.TLSOptions{CertFile: *tlsCert, KeyFile: *tlsKey, CAFile: *tlsCA}
	}

	self := selfURLWithTLS(*addr, tlsOpts != nil)
	cl, err := cluster.New(self, st, tlsOpts)
	if err != nil {
		log.Fatalf("cluster init: %v", err)
	}
	// Hinted handoff: writes that can't reach a replica are stashed in
	// the data dir and replayed when that node returns.
	if err := cl.EnableHints(*dir); err != nil {
		log.Fatalf("enable hints: %v", err)
	}
	if *clusterName != "" {
		cl.SetClusterName(*clusterName)
	}
	srv := api.NewWithRF(self, st, cl, *rf)
	cl.Start()
	defer cl.Stop()

	// Continuous PITR archiving: every interval, copy the raw commit
	// log (full history) to the archive dir under a timestamped name.
	// Recovery: microctl pitr --in <archive> --until <ts> --dir <new>.
	if *archiveDir != "" {
		if err := os.MkdirAll(*archiveDir, 0o755); err != nil {
			log.Fatalf("archive dir: %v", err)
		}
		go func() {
			t := time.NewTicker(*archiveInterval)
			defer t.Stop()
			archiveOnce := func() {
				name := time.Now().UTC().Format("20060102T150405.000") + ".jsonl"
				f, err := os.Create(filepath.Join(*archiveDir, name))
				if err != nil {
					log.Printf("archive create: %v", err)
					return
				}
				defer f.Close()
				if err := st.ArchiveRaw(f); err != nil {
					log.Printf("archive copy: %v", err)
					return
				}
				log.Printf("archived commit log -> %s", name)
			}
			archiveOnce() // one immediately at startup
			for range t.C {
				archiveOnce()
			}
		}()
	}

	if *join != "" {
		seeds := strings.Split(*join, ",")
		go func() {
			// Retry seeds until one answers — gossip only spreads among
			// known peers, so the first successful join is critical.
			// Join now also streams the seed's data (bootstrap) before
			// returning success.
			for {
				for _, seed := range seeds {
					seed = strings.TrimSpace(seed)
					if seed == "" {
						continue
					}
					if err := cl.Join(seed); err == nil {
						log.Printf("joined seed %s; peers: %v", seed, cl.Peers())
						return
					}
				}
				time.Sleep(time.Second)
			}
		}()
	} else {
		// No seed: standalone node, nothing to stream.
		cl.MarkBootstrapped()
	}

	log.Printf("microdb node %s listening on %s (data: %s, tls=%v, rf=%d)", self, *addr, *dir, tlsOpts != nil, *rf)
	var handler http.Handler = srv
	handler = api.RequireAPIAuth(*authToken, handler)
	if *idemTTL > 0 {
		srv.SetIdempotencyTTL(*idemTTL)
	}
	if *tenantsFile != "" {
		reg, err := tenants.Load(*tenantsFile)
		if err != nil {
			log.Fatalf("load tenants: %v", err)
		}
		srv.SetTenants(reg)
		log.Printf("multi-tenancy enabled: %d tenants from %s", len(reg.All()), *tenantsFile)
	}
	if *maxInflight > 0 {
		handler = api.Backpressure(*maxInflight, handler)
	}
	handler = api.Tracing(*traceSlowMs, handler) // outermost: every request gets a trace id
	if tlsOpts != nil {
		handler = api.RequireInternalTLS(handler)
		srvTLS, err := tlsOpts.ServerTLS()
		if err != nil {
			log.Fatalf("tls server config: %v", err)
		}
		// VerifyClientCertIfGiven: public API allows anonymous clients,
		// RequireInternalTLS enforces certs on /internal/*.
		srvTLS.ClientAuth = tls.VerifyClientCertIfGiven
		s := &http.Server{Addr: *addr, Handler: handler, TLSConfig: srvTLS}
		log.Fatal(s.ListenAndServeTLS("", ""))
	}
	if err := http.ListenAndServe(*addr, handler); err != nil {
		log.Fatal(err)
	}
}

// jsonLogWriter turns each log line into a JSON object with a UTC
// timestamp, for log shippers (Loki/CloudWatch/ELK).
type jsonLogWriter struct{ w io.Writer }

func (j *jsonLogWriter) Write(p []byte) (int, error) {
	msg := strings.TrimRight(string(p), "\n")
	rec := map[string]string{"ts": time.Now().UTC().Format(time.RFC3339Nano), "msg": msg}
	b, err := json.Marshal(rec)
	if err != nil {
		return j.w.Write(p)
	}
	if _, err := j.w.Write(append(b, '\n')); err != nil {
		return 0, err
	}
	return len(p), nil
}

// selfURLWithTLS builds the canonical URL other nodes use to reach us.
func selfURLWithTLS(addr string, secure bool) string {
	scheme := "http"
	if secure {
		scheme = "https"
	}
	return selfURLInner(addr, scheme)
}

func selfURLInner(addr, scheme string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
		port = "80"
	}
	if host == "" || host == "0.0.0.0" || host == "::" || host == "[::]" {
		host = "127.0.0.1"
	}
	return "http://" + host + ":" + port
}
