# bench — microdb performance harness

The regression gate for every performance feature in [`FEATURES.md`](../FEATURES.md).
Nothing in the "Speed" tier is claimed as shipped until it shows up here.

## Two halves

| Part | What it measures | Command |
|------|------------------|---------|
| `./` (Go benchmarks) | one code path in isolation: store ops, HTTP handlers, replay | `go test ./bench -bench . -benchmem` |
| `../cmd/microbench` | a whole running node under closed-loop load: throughput + percentiles | `microbench -url http://host:8001` |

## Micro-benchmarks

```bash
go test ./bench -bench . -benchmem -count=1
```

| Benchmark | Path it isolates |
|-----------|------------------|
| `StorePointWrite` / `StorePointWriteFsync` | client write path, buffered vs durable |
| `StorePointWriteFsyncParallel` | group commit: concurrent durable writers sharing log passes |
| `StorePointRead` / `StorePointReadParallel` | point reads, and whether they scale with `-cpu` |
| `StoreBatch100` | batch write path (one log record, one fsync) |
| `StoreReplicaApply` | replication / anti-entropy apply |
| `StoreScanFilter` / `StoreScanIndexed` | full scan vs inverted-index fast path |
| `StoreOpenReplay` | startup cost — rebuild state from the commit log |
| `HTTPWrite` / `HTTPRead` / `HTTPQuery` | the full request path a client actually pays |

`StorePointReadParallel` is the one to watch for lock contention: run it with
`-cpu=1,4,8`. If ns/op does not fall as cores are added, reads are serializing.

## Comparing two revisions

```bash
go test ./bench -bench . -count=10 -benchmem > old.txt
# ...apply the change...
go test ./bench -bench . -count=10 -benchmem > new.txt
benchstat old.txt new.txt
```

(`go install golang.org/x/perf/cmd/benchstat@latest`)

## Load driver

```bash
# 10s mixed read/write, 32 closed-loop workers, JSON report on stdout
go run ./cmd/microbench -url http://127.0.0.1:8001 -duration 10s -workers 32

# reads only against a seeded keyspace
go run ./cmd/microbench -url http://127.0.0.1:8001 -workload read -workers 64

# step load: double concurrency at the halfway point
go run ./cmd/microbench -url http://127.0.0.1:8001 -workload mixed -burst
```

The report is a single JSON object:

```json
{
  "target": "http://127.0.0.1:8001",
  "workload": "mixed",
  "workers": 32,
  "ops": 184322,
  "errors": 0,
  "ops_per_sec": 18432.2,
  "latency": {"mean_ms": 0.71, "p50_ms": 0.52, "p95_ms": 1.4, "p99_ms": 3.1, "max_ms": 41.0}
}
```

Percentiles use nearest-rank over retained samples (cap: `loadgen.DefaultMaxSamples`),
so long runs stay bounded in memory while `ops`/`errors` remain exact.

## Acceptance rule

A speed feature is done only when:

1. `go test ./...` is green **and** `-race` clean;
2. `benchstat` shows the intended movement on the targeted benchmark;
3. no other benchmark regresses by more than noise;
4. `FEATURES.md` row is flipped to ✅ with the numbers as evidence.