// Package bench holds microdb's Go benchmarks — the baseline every
// performance feature in FEATURES.md is judged against.
//
// Run the suite:
//
//	go test ./bench -bench . -benchmem -count=1
//
// Compare two revisions (requires benchstat):
//
//	go test ./bench -bench . -count=10 > old.txt
//	# ...apply the change...
//	go test ./bench -bench . -count=10 > new.txt
//	benchstat old.txt new.txt
//
// A perf feature is only "done" when benchstat shows the intended
// movement and nothing else regresses.
package bench

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"testing"
	"time"

	"microdb/internal/api"
	"microdb/internal/cluster"
	"microdb/internal/index"
	"microdb/internal/store"
)

// openStore returns a store in a temp dir plus a cleanup func.
func openStore(tb testing.TB) (*store.Store, func()) {
	tb.Helper()
	st, err := store.Open(tb.TempDir())
	if err != nil {
		tb.Fatalf("open store: %v", err)
	}
	return st, func() { st.Close() }
}

// seedDocs writes n docs into one collection. Callers wrap this in
// StopTimer/ResetTimer so setup never counts toward ns/op.
func seedDocs(tb testing.TB, st *store.Store, col string, n int) {
	tb.Helper()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("k%06d", i)
		if _, err := st.Apply(col, id, map[string]interface{}{
			"n":    i,
			"age":  i % 90,
			"name": "user-" + id,
		}); err != nil {
			tb.Fatalf("seed %s: %v", id, err)
		}
	}
}

// --- store microbenchmarks -----------------------------------------

// writeKeyspace bounds the id space used by write benchmarks. Writes
// rotate over it so index maintenance cost stays bounded and the
// numbers reflect a steady-state workload rather than a growing map.
const writeKeyspace = 10000

func BenchmarkStorePointWrite(b *testing.B) {
	st, done := openStore(b)
	defer done()
	seedDocs(b, st, "bench", writeKeyspace)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("k%06d", i%writeKeyspace)
		if _, err := st.Apply("bench", id, map[string]interface{}{"n": i}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStorePointWriteFsync measures the durable path (--fsync),
// where every write blocks on a disk sync.
func BenchmarkStorePointWriteFsync(b *testing.B) {
	st, done := openStore(b)
	defer done()
	st.SetFsync(true)
	seedDocs(b, st, "bench", writeKeyspace)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("k%06d", i%writeKeyspace)
		if _, err := st.Apply("bench", id, map[string]interface{}{"n": i}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStorePointWriteCompressed prices the compression flag:
// what a compressible document costs per write versus the raw path.
func BenchmarkStorePointWriteCompressed(b *testing.B) {
	st, done := openStore(b)
	defer done()
	st.SetCompress(true)
	seedDocs(b, st, "bench", writeKeyspace)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("k%06d", i%writeKeyspace)
		if _, err := st.Apply("bench", id, map[string]interface{}{"n": i, "pad": "0123456789abcdef"}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStorePointWriteFsyncParallel shows group commit under
// load: concurrent durable writers share fsync passes instead of each
// paying for one, so throughput should not collapse the way the
// serial fsync benchmark does.
func BenchmarkStorePointWriteFsyncParallel(b *testing.B) {
	st, done := openStore(b)
	defer done()
	st.SetFsync(true)
	seedDocs(b, st, "bench", writeKeyspace)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			id := fmt.Sprintf("k%06d", i%writeKeyspace)
			if _, err := st.Apply("bench", id, map[string]interface{}{"n": i}); err != nil {
				b.Errorf("apply: %v", err)
				return
			}
			i++
		}
	})
}

func BenchmarkStorePointRead(b *testing.B) {
	st, done := openStore(b)
	defer done()
	const n = 10000
	seedDocs(b, st, "bench", n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := st.Get("bench", fmt.Sprintf("k%06d", i%n)); !ok {
			b.Fatalf("miss on key %d", i%n)
		}
	}
}

// BenchmarkStorePointReadParallel is the number a global store lock
// shows up in: run with -cpu=1,4,8 and watch whether ns/op actually
// falls as cores are added.
func BenchmarkStorePointReadParallel(b *testing.B) {
	st, done := openStore(b)
	defer done()
	const n = 10000
	seedDocs(b, st, "bench", n)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, ok := st.Get("bench", fmt.Sprintf("k%06d", i%n)); !ok {
				b.Errorf("miss on key %d", i%n)
				return
			}
			i++
		}
	})
}

func BenchmarkStoreBatch100(b *testing.B) {
	st, done := openStore(b)
	defer done()
	batch := make(map[string]map[string]interface{}, 100)
	for i := 0; i < 100; i++ {
		batch[fmt.Sprintf("k%06d", i)] = map[string]interface{}{"n": i}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := st.ApplyBatch("bench", batch); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStoreReplicaApply is the replication/anti-entropy hot path:
// a peer pushes a doc and we merge + persist it locally.
func BenchmarkStoreReplicaApply(b *testing.B) {
	st, done := openStore(b)
	defer done()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d := &store.Doc{
			ID:         fmt.Sprintf("k%06d", i%writeKeyspace),
			Collection: "bench",
			Ver:        int64(i + 1),
			TS:         time.Now().UnixMilli(),
			Fields:     map[string]interface{}{"n": i},
		}
		st.ApplyRemote(d)
	}
}

const benchScanDocs = 20000

// BenchmarkStoreScanMissCompound is the bloom filter's payoff: a
// compound query whose equality matches nothing used to walk the whole
// collection; it now costs a filter probe. Measured side by side with
// the equivalent full scan.
func BenchmarkStoreScanMissCompound(b *testing.B) {
	st, done := openStore(b)
	defer done()
	seedDocs(b, st, "bench", benchScanDocs)
	filter := map[string]interface{}{"name": "nobody-at-all", "n": 7}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := st.ScanIndexed("bench", filter); len(got) != 0 {
			b.Fatalf("expected no matches, got %d", len(got))
		}
	}
}

// BenchmarkStoreScanMissFullScan is the same query without the bloom
// short-circuit — what it cost before.
func BenchmarkStoreScanMissFullScan(b *testing.B) {
	st, done := openStore(b)
	defer done()
	seedDocs(b, st, "bench", benchScanDocs)
	filter := map[string]interface{}{"name": "nobody-at-all", "n": 7}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got := st.Scan("bench", func(d *store.Doc) bool { return store.Matches(d, filter) })
		if len(got) != 0 {
			b.Fatalf("expected no matches, got %d", len(got))
		}
	}
}

// BenchmarkStoreScanTopK20 vs BenchmarkStoreScanSortAll20 is the
// operator-pushdown claim: a 20-row window over 20k matches.
func BenchmarkStoreScanTopK20(b *testing.B) {
	st, done := openStore(b)
	defer done()
	seedDocs(b, st, "bench", benchScanDocs)
	filter := map[string]interface{}{"n": map[string]interface{}{"$gt": -1}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		docs, _ := st.ScanTopK("bench", filter, nil, "n", false, 20)
		if len(docs) != 20 {
			b.Fatalf("got %d docs", len(docs))
		}
	}
}

// BenchmarkStoreScanSortAll20 is the old shape: materialise every
// match, sort it, cut the window.
func BenchmarkStoreScanSortAll20(b *testing.B) {
	st, done := openStore(b)
	defer done()
	seedDocs(b, st, "bench", benchScanDocs)
	filter := map[string]interface{}{"n": map[string]interface{}{"$gt": -1}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		docs := st.ScanIndexed("bench", filter)
		sort.SliceStable(docs, func(x, y int) bool {
			return store.CompareValues(docs[x].Fields["n"], docs[y].Fields["n"]) < 0
		})
		if len(docs) < 20 {
			b.Fatalf("got %d docs", len(docs))
		}
		docs = docs[:20]
	}
}

func BenchmarkStoreScanFilter(b *testing.B) {
	st, done := openStore(b)
	defer done()
	seedDocs(b, st, "bench", benchScanDocs)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got := st.Scan("bench", func(d *store.Doc) bool {
			n, ok := d.Fields["n"].(int)
			if !ok {
				return false
			}
			return n%7 == 0
		})
		if len(got) == 0 {
			b.Fatal("empty scan")
		}
	}
}

func BenchmarkStoreScanIndexed(b *testing.B) {
	st, done := openStore(b)
	defer done()
	st.Indexes().For("bench").SetIndexedFields([]string{"age"})
	seedDocs(b, st, "bench", benchScanDocs)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got := st.ScanIndexed("bench", map[string]interface{}{"age": 42})
		if len(got) == 0 {
			b.Fatal("empty indexed scan")
		}
	}
}

// BenchmarkIndexAddRemove isolates index maintenance: one document's
// worth of work inside a collection that already holds writeKeyspace
// documents. Before the reverse-map fix this scaled with collection
// size (3.18ms/op at 10k docs); it must stay flat as the collection
// grows.
func BenchmarkIndexAddRemove(b *testing.B) {
	ix := index.New()
	fields := map[string]interface{}{"n": 1, "name": "user", "age": 30}
	for i := 0; i < writeKeyspace; i++ {
		ix.Add(fmt.Sprintf("k%06d", i), fields)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("k%06d", i%writeKeyspace)
		ix.Remove(id)
		ix.Add(id, fields)
	}
}

// BenchmarkStoreOpenReplay measures startup cost: rebuilding
// in-memory state from the commit log. This is the number that gets
// painful as the dataset grows.
func BenchmarkStoreOpenReplay(b *testing.B) {
	dir := b.TempDir()
	probe, err := store.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	const n = 5000
	for i := 0; i < n; i++ {
		if _, err := probe.Apply("bench", fmt.Sprintf("k%06d", i), map[string]interface{}{
			"n": i, "name": "user", "email": fmt.Sprintf("u%d@x.io", i),
		}); err != nil {
			b.Fatal(err)
		}
	}
	probe.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := store.Open(dir)
		if err != nil {
			b.Fatal(err)
		}
		s.Close()
	}
}

// --- HTTP API benchmarks ------------------------------------------
//
// These exercise the full request path: routing, JSON decode/encode,
// index maintenance, replication fanout — the numbers a client feels.

func newAPINode(tb testing.TB) (*store.Store, string, func()) {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}
	addr := "http://" + ln.Addr().String()
	st, err := store.Open(tb.TempDir())
	if err != nil {
		tb.Fatalf("open store: %v", err)
	}
	cl, err := cluster.New(addr, st, nil)
	if err != nil {
		tb.Fatalf("cluster: %v", err)
	}
	srv := api.NewWithRF(addr, st, cl, 1)
	cl.Start()
	httpSrv := &http.Server{Handler: srv}
	go httpSrv.Serve(ln)
	return st, addr, func() {
		cl.Stop()
		httpSrv.Close()
		st.Close()
	}
}

func BenchmarkHTTPWrite(b *testing.B) {
	st, addr, done := newAPINode(b)
	defer done()
	seedDocs(b, st, "bench", writeKeyspace)
	client := &http.Client{Timeout: 5 * time.Second}
	body := []byte(`{"n":1,"name":"alice","age":31}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest(http.MethodPut,
			addr+"/api/collections/bench/docs/"+fmt.Sprintf("k%06d", i%writeKeyspace),
			bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			b.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			b.Fatalf("PUT -> %d", resp.StatusCode)
		}
	}
}

func BenchmarkHTTPRead(b *testing.B) {
	st, addr, done := newAPINode(b)
	defer done()
	const n = 10000
	seedDocs(b, st, "bench", n)
	client := &http.Client{Timeout: 5 * time.Second}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest(http.MethodGet,
			addr+"/api/collections/bench/docs/"+fmt.Sprintf("k%06d", i%n), nil)
		resp, err := client.Do(req)
		if err != nil {
			b.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			b.Fatalf("GET -> %d", resp.StatusCode)
		}
	}
}

func BenchmarkHTTPQuery(b *testing.B) {
	st, addr, done := newAPINode(b)
	defer done()
	seedDocs(b, st, "bench", 5000)
	client := &http.Client{Timeout: 5 * time.Second}
	u := addr + "/api/collections/bench/docs?filter=" +
		url.QueryEscape(`{"age":42}`) + "&limit=20"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest(http.MethodGet, u, nil)
		resp, err := client.Do(req)
		if err != nil {
			b.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			b.Fatalf("query -> %d", resp.StatusCode)
		}
	}
}
