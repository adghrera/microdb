// microcompute: a stateless compute node for microdb.
//
// Holds no authoritative data. Routes client requests to the
// storage-tier nodes via the record API + placement ring. Scale it
// out behind a load balancer; kill it freely — no data to move.
//
// Usage:
//
//	microcompute --addr :8090 --storage http://node1:8081,http://node2:8081
package main

import (
	"flag"
	"log"
	"net/http"
	"strings"

	"microdb/internal/compute"
)

func main() {
	addr := flag.String("addr", ":8090", "listen address")
	storageNodes := flag.String("storage", "", "comma-separated storage node URLs (required)")
	flag.Parse()
	if *storageNodes == "" {
		log.Fatal("--storage is required (comma-separated storage node URLs)")
	}
	nodes := strings.Split(*storageNodes, ",")
	for i := range nodes {
		nodes[i] = strings.TrimSpace(nodes[i])
	}
	eng := compute.New(nodes)
	if err := eng.RefreshPlacement(); err != nil {
		log.Printf("initial placement refresh failed (will retry on demand): %v", err)
	}
	log.Printf("microcompute listening on %s (storage: %v)", *addr, eng.Nodes())
	log.Fatal(http.ListenAndServe(*addr, eng.Handler()))
}
