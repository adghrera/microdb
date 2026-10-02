// microbench drives load against a running microdb node and prints a
// JSON latency/throughput report — the multi-node half of the
// benchmark harness (the micro-benchmarks live in ./bench).
//
// Start a node, then:
//
//	microbench -url http://127.0.0.1:8001 -workload mixed -duration 10s
//	microbench -url http://127.0.0.1:8001 -workload read  -workers 64
//	microbench -url http://127.0.0.1:8001 -workload query -burst
//
// Output is JSON (one object) so it can be archived per commit and
// diffed: a performance feature is only accepted when this report, or
// a benchstat comparison from ./bench, shows the intended movement.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"microdb/bench/loadgen"
)

func main() {
	var (
		url        = flag.String("url", "http://127.0.0.1:8001", "base URL of the node under test")
		token      = flag.String("token", "", "bearer token (if the API requires one)")
		collection = flag.String("collection", "bench", "collection to hit")
		duration   = flag.Duration("duration", 10*time.Second, "how long to run")
		workers    = flag.Int("workers", 32, "closed-loop workers (concurrency)")
		workload   = flag.String("workload", "mixed", "mixed | read | write | query")
		readPct    = flag.Int("read-pct", 50, "read share inside the mixed workload")
		keyspace   = flag.Int("keyspace", 10000, "distinct document ids")
		limit      = flag.Int("limit", 20, "query result limit (workload=query)")
		burst      = flag.Bool("burst", false, "double concurrency at the halfway point")
		seed       = flag.Bool("seed", false, "force seeding of the keyspace (on by default for read/query)")
		quiet      = flag.Bool("quiet", false, "print only the JSON report")
	)
	flag.Parse()

	rep, err := loadgen.Run(loadgen.Config{
		URL:        *url,
		Token:      *token,
		Collection: *collection,
		Duration:   *duration,
		Workers:    *workers,
		Workload:   *workload,
		ReadPct:    *readPct,
		Keyspace:   *keyspace,
		QueryLimit: *limit,
		Burst:      *burst,
		Seed:       *seed,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "microbench: %v\n", err)
		os.Exit(2)
	}

	out, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "microbench: marshal: %v\n", err)
		os.Exit(2)
	}
	fmt.Println(string(out))

	if !*quiet {
		fmt.Fprintf(os.Stderr,
			"\n%s | %d workers | %s\n  throughput : %.0f ops/s (%d ops, %d errors)\n"+
				"  latency    : p50 %.2fms  p95 %.2fms  p99 %.2fms  max %.2fms\n",
			rep.Workload, rep.Workers, rep.Target,
			rep.OpsPerSec, rep.Ops, rep.Errors,
			rep.Latency.P50, rep.Latency.P95, rep.Latency.P99, rep.Latency.Max)
	}
	// A run where every request failed is a failed run, not a result.
	if rep.Errors > 0 && rep.Ops == rep.Errors {
		os.Exit(1)
	}
}
