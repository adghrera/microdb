// microlog — a durable, quorum-replicated shared commit log service.
//
// Storage nodes (microdb) write THROUGH this log instead of relying
// on their own local fsync for durability. Run an odd number of
// log nodes (typically 3) that can see each other; an append is
// ACKed only after a majority have persisted it.
//
// Usage:
//
//	microlog --addr :9001 --self http://127.0.0.1:9001 \
//	         --members http://127.0.0.1:9001,http://127.0.0.1:9002,http://127.0.0.1:9003 \
//	         --dir ./log1
//
// API:
//
//	GET  /log/health            liveness + current LSN + believed leader
//	POST /log/append            {"payload": <json>}      -> {"lsn": N}
//	POST /log/replicate         {"lsn":N,"ts":..,"payload":..} (internal)
//	GET  /log/tail?after=N&limit=M  -> {"entries":[...], "last_lsn": N}
package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"strings"

	"microdb/internal/sharedlog"
)

func main() {
	addr := flag.String("addr", ":9001", "listen address")
	self := flag.String("self", "", "this node's URL as peers reach it (default: derived from --addr)")
	members := flag.String("members", "", "comma-separated ordered log node URLs")
	dir := flag.String("dir", "./logdata", "log directory")
	flag.Parse()

	if *members == "" {
		log.Fatal("--members is required (comma-separated log node URLs)")
	}
	selfURL := *self
	if selfURL == "" {
		selfURL = selfURLFromAddr(*addr)
	}
	ms := splitList(*members)
	srv, err := sharedlog.NewServer(ms, selfURL, *dir)
	if err != nil {
		log.Fatalf("shared log server: %v", err)
	}
	log.Printf("microlog %s listening on %s (members: %v, leader: %s, lsn: %d)",
		selfURL, *addr, ms, srv.Leader(), srv.LastLSN())
	if err := http.ListenAndServe(*addr, srv.Handler()); err != nil {
		log.Fatal(err)
	}
}

func splitList(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func selfURLFromAddr(addr string) string {
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
