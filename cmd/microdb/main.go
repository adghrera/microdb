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
	"flag"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"microdb/internal/api"
	"microdb/internal/cluster"
	"microdb/internal/store"
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
	flag.Parse()

	st, err := store.Open(*dir)
	if err != nil {
		log.Fatalf("open store: %v", err)
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
	srv := api.NewWithRF(self, st, cl, *rf)
	cl.Start()
	defer cl.Stop()

	if *join != "" {
		seeds := strings.Split(*join, ",")
		go func() {
			// Retry seeds until one answers — gossip only spreads among
			// known peers, so the first successful join is critical.
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
	}

	log.Printf("microdb node %s listening on %s (data: %s, tls=%v, rf=%d)", self, *addr, *dir, tlsOpts != nil, *rf)
	var handler http.Handler = srv
	handler = api.RequireAPIAuth(*authToken, handler)
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
