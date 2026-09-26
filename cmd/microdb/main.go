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
	flag.Parse()

	st, err := store.Open(*dir)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	self := selfURL(*addr)
	cl := cluster.New(self, st)
	srv := api.New(self, st, cl)
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

	log.Printf("microdb node %s listening on %s (data: %s)", self, *addr, *dir)
	if err := http.ListenAndServe(*addr, srv); err != nil {
		log.Fatal(err)
	}
}

// selfURL builds the canonical http URL other nodes use to reach us.
func selfURL(addr string) string {
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
