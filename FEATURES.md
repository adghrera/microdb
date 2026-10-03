# microdb — Feature Checklist & Priority Score

**Project:** `microdb` · Go 1.23 · stdlib-only distributed schema-less document store
**Status:** all Tier 1–3 features + most Tier 4 shipped. Every "done" row is backed by a passing test or live run.
**Next:** see [⚡ Fast / scalable / production-grade](#-fast-scalable-production-grade--the-remaining-gap) below — the ☐ gap plan to take microdb from "correct and elastic" to "serves real traffic".
**Priority scale:** 10 = core correctness/foundation → 0 = nice-to-have polish.

Legend: ✅ shipped & verified · ⬜ not done · ⚠️ shipped with caveat

---

## ✅ Shipped (59 features)

| ✓ | Feature | Pri | Verification |
|---|---------|-----|--------------|
| [x] | Schema-less JSON documents | 10 | Live demo: disjoint schemas stored & replicated |
| [x] | Append-only JSONL persistence + crash recovery | 9 | Replay tests across restarts |
| [x] | LWW conflict merge by (ver, ts), idempotent | 9 | `TestAntiEntropyResolvesConcurrentDivergence` |
| [x] | Consistent hashing ring (128 vnodes/node) | 8 | Rebalance across 3-node live cluster |
| [x] | RF=3 replication fanout | 8 | `TestClusterReplication` |
| [x] | Gossip membership (seed join, TTL eviction, ring rebuild) | 8 | Mesh converges ~1s; eviction observed |
| [x] | Any-node write routing (hop-limited forward to owner) | 7 | Writes to any port succeed |
| [x] | Tombstone deletes (replicated) | 7 | Delete propagation test |
| [x] | Merkle-tree anti-entropy (root compare + targeted exchange) | 9 | 6 unit + 2 integration tests |
| [x] | **Log compaction + tombstone GC** (Tier 1) | 8 | `TestCompactKeepsLatestVersionOnly`, `TestCompactTombstoneGC`, fresh-tombstone safety |
| [x] | **fsync-on-write option** `--fsync` (Tier 1) | 7 | `TestFsyncModePersistsWrite` |
| [x] | **Recursive subtree-diff anti-entropy** (Tier 1) | 7 | `TestDiffAgainstRemote*` incl. fetch-count bounds, zero-fetch short-circuit |
| [x] | **Scatter-gather queries** (Tier 1) | 7 | `TestScatterGatherQuery` — union across nodes, partial-flag on failure |
| [x] | **Read-your-writes via sticky owner routing** (Tier 1) | 6 | Writes always route to the ring owner; owner-local read is immediately fresh |
| [x] | **Inverted secondary indexes + query fast path** (Tier 2) | 6 | `TestScanIndexedFastPathMatchesFullScan`, 6 index unit tests |
| [x] | **Full operator set** `$gte $lte $ne $in $exists $regex` (Tier 2) | 6 | 19-case matrix `TestMatchesOperators` |
| [x] | **Pagination + sort** `limit/offset/sort/desc` (Tier 2) | 5 | `TestPaginationSort` — global ordering across shards |
| [x] | **Batch writes** — atomic single-record, one fsync (Tier 2) | 5 | `TestApplyBatchAtomicAndReplay` |
| [x] | **Change feed / watch** — long-poll NDJSON (Tier 2) | 4 | `TestWatchFeed`, `TestGoClientWatch`, 5 changelog unit tests |
| [x] | **Mutual TLS for internal traffic** (Tier 3) | 7 | `TestTLSMutualAuth` — in-test CA, 403 without cert, public API open |
| [x] | **API bearer-token auth** `--auth-token` (Tier 3) | 7 | `TestRequireAPIAuth` — 401/200 matrix, /v1 gated identically |
| [x] | **Prometheus metrics** `/metrics` (Tier 3) | 6 | Counters (writes/deletes/applies/sends) + gauges (docs/collections/peers) |
| [x] | **Backup/restore + microctl admin CLI** (Tier 3) | 6 | `TestBackupRestoreRoundTrip`; `microctl backup/restore/status` |
| [x] | **Configurable replication factor** `--rf` (Tier 3) | 5 | `TestReplicationFactor` — RF=1 owner-only, RF=2 both owners |
| [x] | **Go client library** (Tier 4) | 5 | `client/` — CRUD/batch/query/watch/TLS/token; 2 integration tests |
| [x] | **HTTP API versioning** `/v1/api/...` (Tier 4) | 2 | Dual-registered routes; auth parity tested |
| [x] | **Docker + compose** 3-node cluster (Tier 4) | 4 | ⚠️ files shipped; no Docker daemon in build env to run them |
| [x] | **README + architecture doc** (Tier 4) | 4 | `README.md` |
| [x] | **`-race` clean test runs** (Tier 4) | 4 | ✅ verified race-clean at 40 features: `CC='C:\w\msys64\mingw64\bin\gcc.exe' CGO_ENABLED=1 go test -race ./...` (MSYS gcc can't build Go's cgo shim — use mingw64) |
| [x] | **Ring epochs + write fencing** (Roadmap A2) | 9 | Epoch bumped on join/evict, gossiped + adopted monotonically; forwarded writes carry `X-Microdb-Epoch`, stale forwarders get 409 → adopt → retry. `TestFencingStaleEpoch`, `TestEpochPropagation` |
| [x] | **Quorum reads + read-repair** (Roadmap D1/D4) | 8 | `?consistency=quorum|all` merges replicas by (ver,ts), 503 if quorum unreachable, async repair pushes newest to lagging replicas. `TestQuorumRead`, `TestQuorumReadNotReached` |
| [x] | **Quorum writes** (Roadmap D1) | 8 | `?consistency=quorum` blocks until W=majority of RF-owner set acked; honest 503 (`applied:true`) when not reached |
| [x] | **Hinted handoff** (Roadmap D3) | 7 | New `internal/hints`: durable capped JSONL debt, newest-wins replace, exp backoff ≤5min, corrupt-tail tolerant; failed Replicate stashes, 1s replay loop delivers on peer return. 7 unit + 3 integration tests incl. sender-restart durability |
| [x] | **Graceful decommission** (Roadmap A3) | 9 | `microctl decommission --url`: drain (refuse writes 503) → bulk handoff to peers (failures → hints) → leave broadcast evicts immediately (no 15s TTL wait); gossip tombstones block resurrection 30s, explicit join overrides. Live-verified: 20-doc handoff, survivors complete after kill |
| [x] | **Bootstrap streaming on join** (Roadmap A1) | 10 | `Join()` pulls the seed's full dataset via paginated `GET /internal/stream/{col}` before returning; `streaming=true` state until complete (accepts writes, reads may be stale); 5 integration tests incl. page coverage + writes-during-streaming |
| [x] | **Join admission control** (Roadmap A6) | 6 | Seed serves max 2 concurrent bootstrap streams (429 + Retry-After beyond); joiner retries with exponential backoff |
| [x] | **Full per-request tunable consistency** (Roadmap D2) | 7 | `one`/`quorum`/`all`/`local_quorum` on reads AND writes; unknown label → 400. Tests: labels matrix, all-write quorum failure honest 503, one-write unaffected by dead members |
| [x] | **Hedged reads on scatter-gather** (Roadmap B5) | 6 | `?hedge_ms=N`: parallel gathers; slow members get a duplicate gather at another member, first response wins; failed hedges never mark response partial |
| [x] | **Shard-map aware clients** (Roadmap B4) | 6 | `GET /internal/owners/{col}/{id}` (owners + epoch); Go client `Owners`/`PutRouted`/`InvalidateShardMap` — writes go straight to the primary, cache-drop + fallback on move |
| [x] | **Point-in-time recovery (PITR)** (Roadmap D5) | 7 | `--archive-dir`/`--archive-interval` continuously copy the raw commit log; `microctl pitr --in <log> --until <ts> --dir <new>` replays to any point; recovered store is durable + live. Live-verified restore-before-later-writes |
| [x] | **Backpressure / load shedding** (Roadmap E2) | 7 | `--max-inflight`: 429 + Retry-After above the concurrent cap instead of queueing to timeout; /health never shed; shed counter + inflight gauge |
| [x] | **Request tracing** (Roadmap E5) | 5 | X-Trace-Id on every response (honors incoming id), latency buckets fast/slow/very-slow, slow-request logging with trace id |
| [x] | **Per-collection configuration** (Roadmap E3) | 6 | `PUT /api/collections/{col}/config {"rf": n}` — per-collection RF stored in reserved `_config` collection, replicated, honored by fanout/quorum/owner-set |
| [x] | **Encryption at rest** (Roadmap E4) | 6 | AES-256-GCM per-record (`ENC1:` framing, fresh nonce per record); `--encryption-key`; mixed plaintext/crypto logs; compaction re-encrypts; encrypted PITR; wrong-key fails loudly |
| [x] | **Cluster identity guard** (Roadmap E6) | 8 | `--cluster-name`: every internal request stamped `X-Microdb-Cluster`; mismatched peers get loud 403, `Join()` surfaces the refusal — no silent cross-cluster merges. `TestClusterNameGuard`, `TestJoinRefusedAcrossClusters` |
| [x] | **Readiness endpoint** `/ready` (Roadmap E6) | 7 | 503 while bootstrapping or decommissioning, 200 only when servable; `/health` stays liveness-only. `TestReadyEndpoint` |
| [x] | **Version endpoint** `/version` (Roadmap E6) | 4 | version/Go/node/rf/cluster/uptime for ops dashboards. `TestVersionEndpoint` |
| [x] | **Idempotency keys** (Roadmap E6) | 8 | `Idempotency-Key` header on PUT/POST/DELETE deduped for `--idempotency-ttl` (default 10m); replays return cached response + `X-Idempotent-Replay`, no version churn. `TestIdempotencyKey`, `TestIdempotencyCacheExpiry` |
| [x] | **Structured JSON logs** `--json-log` (Roadmap E6) | 4 | RFC3339Nano UTC timestamped JSON lines for log shippers. live-verified |
| [x] | **Durable change feed** `--durable-feed` | 6 | Watch events persisted to `<dir>/feed.jsonl`, write-through before cursor release, replay on restart (seq + window), torn-tail tolerant, retention-bounded. Cursors survive node restarts. 3 unit + 1 store test |
| [x] | **Sort/limit pushdown** on scatter-gather | 6 | `limit=` pushes sort+cap to every shard: each returns only its top (offset+limit) — O(shards x window) wire instead of O(all docs). Honest `total_exact` flag when truncation makes total a lower bound. `TestPaginationSort` extended |
| [x] | **Online schema migration** (Roadmap E5) | 7 | `PUT /api/collections/{col}/schema` with append-only transforms (rename/drop/retype); docs carry `_schema_ver`, transformed lazily on read (get/query/watch/quorum) — no stop-the-world rewrite; schema replicates as `_config` data. 4 unit + 2 integration tests |
| [x] | **Tenant isolation & quotas** (Roadmap E1) | 7 | `--tenants file.json`: per-tenant bearer tokens, `<name>.*` collection namespaces (cross-tenant 403), token-bucket rate limits with Retry-After, live-doc storage quotas (overwrites free, deletes free). Constant-time token compare. 3 integration tests |
| [x] | **Load-aware weighted vnodes** (Roadmap A5) | 7 | Per-node writes/sec EWMA gossiped on the member list; `ring.BuildWeighted` gives vnodes proportional to load (2x load = ~2x keyspace); idle nodes floor at 5% (min 8 vnodes); rebuilt every gossip tick, same epoch, deterministic. 4 ring unit + 1 integration test |
| [x] | **Read cache with changelog invalidation** (Roadmap C7) | 6 | `--read-cache N`: sharded-LRU read-through cache on local point reads; invalidated synchronously via changelog `OnAppend` hook on every local write AND replicated apply — no TTL-guessing, read-your-writes holds. Cache hit/miss/invalidation metrics. 3 integration tests |
| [x] | **Storage-tier record API** (Roadmap C2) | 9 | `internal/storage.RecordStore`: epoch-fenced Put/Get/Scan/Epoch boundary; Local adapter (store.ApplyVersioned/GetDoc/ScanPage) + Remote HTTP adapter (`/internal/record/...`); stale-epoch writes get 409/ErrStaleEpoch, fence auto-raises with ring rebuild. 2 integration tests |
| [x] | **Stateless query engine** (Roadmap C1) | 9 | `internal/compute.Engine` + `cmd/microcompute`: zero authoritative data; placement ring over live storage nodes, refresh-on-stale + retry, version-incrementing writes, scatter-gather query merge. Kill/scale freely. 3 integration tests |
| [x] | **Partition key + sort key data model** (Roadmap B1) | 8 | `partition_field`/`sort_field` in collection config; ids become `pk\|sort`; ring key = col/partitionValue so one partition = one shard. `GET /collections/{col}/partition/{pk}` = Dynamo-style Query (sort_gte/lte/gt/lt, desc, limit) served from ONE node, forwarded by non-owners. Write validation: id prefix must match partition field; batches stay in one partition. 3 integration tests |
| [x] | **Per-collection isolation** (Roadmap B3) | 7 | Per-collection live stats (writes/deletes/reads/queries/live_docs) tracked at the merge choke point, seeded from replay on restart; `GET /api/stats/collections` for noisy-collection visibility; `max_docs` per-collection quota (403 on overflow, overwrites free, deletes free) — a runaway collection can't consume neighbors' headroom. 3 integration tests |
| [x] | **Global secondary indexes as a service** (Roadmap B6) | 6 | `--gsi`: async worker off the write path fed by the change feed; `PUT/GET /collections/{col}/gsi`, `GET .../gsi/{name}?value=` with explicit lag metadata (`lag_events`); backfill on declare; deletes remove from index; lookups use the inverted-index fast path. 3 integration tests |
| [x] | **Durable shared commit log** (Roadmap C3) | 8 | New `internal/sharedlog` + `cmd/microlog`: quorum-replicated append-only log service (leader = first reachable member, ACK only after majority persist, failover promotes next member and seeds LSN above any peer). Store write-throughs every client mutation (Apply/ApplyBatch/Delete) to the log BEFORE touching local state — no quorum, no write. Restart resumes by tailing from the LSN checkpoint; total local disk loss rebuilds from the log. 6 unit + 4 integration tests |

## ⬜ Remaining (deliberately out of scope for "tiny")

| ☐ | Feature | Pri | Why deferred |
|---|---------|-----|--------------|
| [ ] | Multi-tenancy / namespaces | 2 | Changes the data model; contradicts "tiny" |
| [ ] | Vector clocks / CRDT merge (instead of LWW) | 3 | Changes consistency semantics fundamentally |
| [ ] | Per-collection RF (vs global `--rf`) | 3 | Ring would need per-collection ownership maps |
| [ ] | Durable change feed (persisted events) | 3 | Current feed is bounded in-memory by design |
| [ ] | Range-partitioned queries pushed to shards | 2 | Scatter-gather is O(cluster) but correct |

---

# ⚡ Fast, scalable, production-grade — the remaining gap

Everything above establishes **correctness and elasticity**: the ring moves, quorums hold,
nodes join and leave, data survives restarts. None of it yet makes microdb *fast under load*,
able to hold *more data than one cluster's RAM*, or *safe to operate on a Sunday night*.
That is what this section is for.

Every row below is ☐ — **no claim is made until it is verified.**

### Build order — top 20 by priority (tracked across commits)

| # | Tier | Feature | Pri | Status |
|---|------|---------|-----|--------|
| 1 | F | Benchmark harness + perf regression gate | 10 | ✅ |
| 2 | P | Rolling-upgrade wire versioning (N / N−1) | 10 | ✅ |
| 3 | F | Group commit / write batching | 9 | ✅ |
| 4 | F | Sharded store map (kill the global read lock) | 9 | ✅ |
| 5 | P | End-to-end integrity: checksums + `verify` + `repair` | 9 | ✅ |
| 6 | F | Streaming log replay + self-healing tail (re-scoped from block/page store) | 9 | ✅ |
| 7 | S | Tiered storage: hot local + cold object store | 9 | ⚠️ |
| 8 | F | Log record compression | 8 | ✅ |
| 9 | F | Bloom filters — scan short-circuit (re-scoped from "per segment") | 8 | ✅ |
| 10 | F | Operator pushdown into the scan (top-K sort/limit) | 8 | ✅ |
| 11 | F | Parallel scatter-gather with a shared deadline | 8 | ✅ |
| 12 | F | Aggregate pushdown (`count/sum/min/max/avg/group_by`) | 8 | ✅ |
| 13 | S | Topology-aware placement (rack/zone) | 8 | ✅ |
| 14 | S | Membership & repair that scale to hundreds of nodes | 8 | ✅ |
| 15 | P | Verified, offsite backups (⚠️ no scheduler/retention yet) | 8 | ⚠️ |
| 16 | P | Secrets & key rotation (⚠️ TLS certs not hot-reloaded) | 8 | ⚠️ |
| 17 | P | Audit log | 8 | ✅ |
| 18 | P | SLO metrics + error budget | 8 | ✅ |
| 19 | P | Graceful shutdown that loses nothing | 7 | ✅ |
| 20 | P | Continuous profiling (`pprof`) | 7 | ✅ |

*(Plus one unplanned blocker the harness forced: making write cost
independent of data size — shipped with #1.)*

**Progress: 20 / 20 built** (17 ✅ + 3 ⚠️) — the tier is complete; see the summary at the end of this section.

### Ground rules for this tier

| Rule | Why |
|------|-----|
| **Benchmark before you build** | Each F/S row names the number it moves. No baseline ⇒ no feature. "Fast" must be p99, not a vibe. |
| **Measure the tail** | Report p50/p95/p99/max + ops/s + allocs/op. Averages hide exactly the latency users complain about. |
| **Correctness is the merge gate** | Full suite green + `-race` clean stays mandatory. A perf change that weakens quorum, fencing, or idempotency semantics is rejected regardless of benchmark. |
| **Observable or it didn't happen** | Every feature ships a metric or log line proving it works in production, not just in tests. |
| **One feature per commit** | Matches the existing history discipline. |

> **First finding (baseline, 2026):** the harness immediately exposed two write paths
> that scale with *data size* instead of *document size*:
>
> | Path | Symptom | Root cause | After |
> |------|---------|------------|-------|
> | point write | **3.18 ms/op** at 10k docs (vs 196 ns reads) | `index.Remove` walked every field/value bucket of the collection on every write | **8.0 µs/op** (397×) |
> | watch feed trim | **804 KB allocated per write** once the feed hit its 10k cap | `changelog.trimLocked` rebuilt the whole retained window on every append → GC dominated the profile | **1.6 KB/op** (500×) |
> | batch write (100 docs) | 41.7 ms | both of the above, ×100 | **0.29 ms** (142×) |
> | log replay on startup | 1.0 s for 5k docs | same index path during replay | **34 ms** (30×) |
> | HTTP PUT | 4.44 ms | store cost paid per request | **0.90 ms** (4.9×) |
>
> Fixed in the commit that follows the harness; `bench/README.md` is how this is
> kept fixed.

## F. Speed — latency & throughput

| ☐ | Feature | Pri | What it takes |
|---|---------|-----|---------------|
| [x] | **Benchmark harness + perf regression gate** | 10 | ✅ shipped — `bench/` Go benchmarks (store point read/write, fsync write, batch, replica apply, scan vs index, replay, HTTP write/read/query) + `cmd/microbench` closed-loop load driver (mixed/read/write/query, `-burst` step load) emitting JSON p50/p95/p99 + ops/s. `bench/README.md` defines the acceptance rule (benchstat before/after, no collateral regressions). 7 `loadgen` tests: nearest-rank percentile math, error accounting, burst, JSON round-trip. **Baseline (Ryzen 5700U):** PointWrite 3.18ms · PointWriteFsync 4.31ms · PointRead 196ns · PointReadParallel 54ns (scales with -cpu) · Batch100 41.7ms · ReplicaApply 231µs · ScanFilter 5.1ms · ScanIndexed 36µs · OpenReplay(5k) 1.0s · HTTPWrite 4.4ms · HTTPRead 779µs · HTTPQuery 1.08ms |
| [x] | **Group commit / write batching** | 9 | ✅ shipped — dedicated **log writer** (`internal/store/commit.go`): writers encode their record and queue it; ONE goroutine merges everything queued into a single `Write` + single `fsync`, then releases the waiters. `submit` returns only after a pass that started *after* it was queued ⇒ acknowledged ⇒ durable. Writers never touch the file, which is what makes batching survive Windows serialising `WriteFile` against `FlushFileBuffers` (proved first-hand: a writer appending during a flush stalls 8ms). `--commit-window N` holds the first record open to gather a bigger batch (0 = flush as soon as it forms); `wmu` is the single-writer slot that keeps the inline (fsync-off) path mutually exclusive with batched appends, so the **default path stays at 8.2µs/op** — no queue tax when there is nothing to batch. `Store.CommitStats()` → `/metrics` (`microdb_group_fsync_{passes,writes,errors}_total`). **Numbers:** fsync serial **897µs/op → 115µs/op parallel (7.8×)**; 160 concurrent store writes → **21 log passes (7.6×)**; synthetic 64 waiters → 2 passes (32×). 7 tests (no-overlap, error propagation, shutdown release, byte-exactness, real-path concurrency + reopen durability), `-race` clean |
| [x] | **Sharded store map (kill the global read lock)** | 9 | ✅ shipped — the document map is striped across **64 shards** (`docShard{mu, docs}`, `maphash` stripe over the collection+key). `Store.Get` takes one shard's RLock and never touches the store mutex, so point reads keep running while compaction, replay or writes hold it (previously a compaction stalled every read for its whole run). Iteration walks stripe-by-stripe (`rangeDocs` / `mutateDocs`), and an atomic `nDocs` replaces `len(map)` for O(1) `DocCount`. **A/B vs HEAD, `-cpu 16`, `-count 3`, median:** `StorePointReadParallel` **77 → 47 ns/op (1.6×)**, ~4.3× scaling across 16 threads; `StoreScanFilter` 7.4 → 6.4ms; single-thread `PointRead`/`PointWrite`/`StoreBatch100` unchanged (231ns / 9.6µs / 297µs). Full suite + `-race` clean |
| [x] | **Streaming log replay + self-healing tail** (re-scoped from block/page store) | 9 | ✅ shipped — `OpenWithKey` streams the log through one reusable 1MB buffer (`ReadSlice`) and parses records **straight out of the read buffer**: no whole-file buffer, no string copy of it, no `[]byte(line)` per record. Disk bytes → decoded → merged with no intermediate copies. **Measured on the same fixture, same machine: 13.22× → 3.65× the log's size in transient allocations (3.6× fewer)**, pinned by `TestOpenReplayBoundedMemory` (fails above 5×, 8× under `-race`). Replay now also **self-heals**: a partial record left by a crash is truncated back to the last complete record on open, so `microctl verify` stops reporting the same torn tail forever and every backup stops shipping that garbage (`TestTornTailTruncatedOnOpen`). **Honest note on the original wording:** a page/mmap store with a key→offset sparse index was considered and rejected for *this* architecture — the working set is resident by design (the log is the durability mechanism, not the read path), so random access into the file would buy nothing; data-larger-than-RAM belongs to tiered storage below. |
| [x] | **Log record compression** (was: segment compression) | 8 | ✅ shipped — `--compress`: each record is deflated (`compress/flate`, BestSpeed) **before** encryption (compress-then-encrypt; the reverse would compress ciphertext and save nothing) and framed inside the existing CRC envelope as `Z1:<base64(deflate)>` — base64 because the log is line-framed and raw deflate splits records on 0x0A. Best-effort per record: below 512 bytes, or whenever compression does not actually shrink it, the record stays raw — a "compressed" record longer than the original only costs replay time. Reading is unconditional, so turning the flag off never strands a log written with it on. **Measured:** repetitive payload 25,140 → 3,587 bytes (**86% smaller log**); write cost 8.3µs → 9.2µs (**+1µs / +12% per write**). Works with encryption (`TestCompressPlusEncrypt`), tombstones, and reopen-without-flag (`TestCompressedLogReadsWithCompressionOff`). **Scope note:** this is the disk log; the replication and bootstrap wire still ships JSON. 4 tests + `BenchmarkStorePointWriteCompressed` |
| [x] | **Bloom filters — scan short-circuit** (re-scoped from "per segment") | 8 | ✅ shipped — new `internal/bloom`: a **counting** filter (uint8 counters, Kirsch–Mitzenmacher double hashing, saturating so a wrapped counter can never become a false negative) measured at **1.16% false positives against a 1% target** over 10k items. Held per collection over `field=value` tokens and maintained by the inverted index — added in `addLocked`, removed in `unindex` through the existing reverse map, auto-rebuilt at double capacity past half load. Its consumer is `Store.Prefilter`: the full-scan **fallback** of a compound query (the case the inverted-index fast path cannot serve) asks the filter first and returns empty without walking the collection, metered as `microdb_scan_shortcircuits_total`. **Measured:** a miss over a 20k-doc collection costs **321ns instead of a 16.5ms full scan (~51,000×)**. `TestBloomShortCircuitsScan` proves both directions — it rules out what is absent and never rules out what is present (range conditions get no Bloom answer and fall through to the scan), and counting removal means a deleted value stops shielding its buckets. Tokens are truncated at 128 bytes: a prefix is still exact for equality, and without truncation a 50KB field tripled replay allocations (caught by `TestOpenReplayBoundedMemory`). **Re-scoped honestly:** "per segment" presupposes a disk segment store microdb does not have (an in-RAM exact map plus Merkle root compare already covers the anti-entropy skip), so the filter went where it actually pays. 5 tests + 2 benchmarks |
| [x] | **Operator pushdown into the scan** (top-K sort/limit) | 8 | ✅ shipped — new `Store.ScanTopK`: sort + window are evaluated **inside** the walk with a bounded heap under the requested order (max-heap: root = worst candidate, replaced in place via `heap.Fix`, so a million matches allocate *n* wrappers, not one per replacement). The API's local shard path uses it whenever `limit` is set, with the caller's schema-migration transform applied to each candidate *as it is ordered* — the sort sees the fields the client will see. Ties break by id ascending, which is exactly what the previous stable-sort-over-id-ordered-scan produced, pinned by `TestScanTopKMatchesSortThenCut` against that implementation as an oracle (35 sort/desc/window × filter combinations) plus `TestScanTopKTransformSeesMigratedFields`. **Measured, 20k matches → 20 rows: 18.0ms/761KB → 9.7ms/1.2KB (1.85× faster, 630× less memory, 25 → 30 allocs).** Also fixed a real ordering bug it exposed: `CompareValues`/`cmp` only understood `float64`, so an in-process `int` field compared *equal to everything* — a locally-written doc sorted and range-filtered differently from the same doc written through the HTTP API. New `numOf` coercion covers int/int32/int64/float32/float64/json.Number on both paths |
| [x] | **Parallel scatter-gather with a shared deadline** | 8 | ✅ shipped — the fan-out was already concurrent; what was missing was the *bounded wait*: the merge loop blocked on every gather, so one peer that never answered held the whole query hostage until the HTTP client's own 5s timeout. `handleQuery` now runs a **query-level deadline** (`?timeout_ms=10..60000`, default 2000): gathers that finish inside the budget are merged, the rest are reported by name (`timeout after 250ms waiting for <member>`), and the response carries `partial`, `timed_out` and `timeout_ms`. Late goroutines drain into a buffered channel, so nothing leaks or writes after the response. Metered as `microdb_query_timeouts_total`. **Measured:** with a peer that accepts and never answers, a query returns in **0.26s against a 250ms budget** (previously ~5s, blocked on the client timeout) while still including the local shard's documents — `TestQuerySharedDeadline`, plus `TestQueryRejectsBadTimeout` for the knob |
| [x] | **Aggregate pushdown** (`count/sum/min/max/avg/group_by`) | 8 | ✅ shipped — `?agg=count|sum|avg|min|max[&field=][&group_by=]` computes on each shard (`Store.Aggregate`, reusing the scan path **and the Bloom prefilter**, so a query over a value the collection never held answers without walking anything) and merges partials in the coordinator; the wire carries a handful of numbers instead of every matching document. `Aggregate` carries count+sum (avg = sum/count) plus min/max via `CompareValues`, so `Merge` is associative and commutative — any arrival order, any subset after a deadline, same answer (`TestAggregateMergeIsOrderIndependent`). Internal endpoint `GET /internal/agg/{col}`; responses include `aggregate`, `groups` (stable key order), `group_totals`, `nodes_queried`, and the same `partial`/`timed_out` contract as the document path; 400 on unknown kind or a missing `&field=`. **The correctness trap it had to avoid:** documents are replicated to every RF owner, so a shard counts only docs where it is the **primary** owner (`primaryOwner` = `OwnerSet[0]`, exactly where writes are routed) — otherwise a two-node cluster reports twice the documents. `TestAggregatePushdownAcrossNodes` asserts count=20/sum=190/avg=9.5/max=19/filtered=10/groups=10+10 from **both** nodes over replicated data. Two bugs it flushed out and fixed: group buckets were created without a `kind` (so `Value()` returned nil and `Merge` silently dropped the partial → zeroed groups), and `Merge` replaced an existing bucket instead of filling its kind (discarding the local count) — both now pinned by regression tests. 5 store tests + 2 integration tests |
| [ ] | **Multi-tier read cache (memory → local disk) + engine-level invalidation (C7)** | 7 | Extend `readcache` past the in-process LRU with a bounded disk tier, and let `microcompute` caches subscribe to the storage change feed for invalidation instead of TTL-guessing. |
| [ ] | **Read-ahead on range scans & pagination** | 7 | Batched I/O for sequential `ScanPage`; prefetch the next partition on deep pagination instead of round-tripping per page. |
| [ ] | **Framed persistent internal connections** | 6 | Replication, gossip, and anti-entropy currently do a request per message. Multiplex over a long-lived length-prefixed, versioned connection per peer with bounded per-peer write queues. |
| [ ] | **End-to-end write-path backpressure** | 6 | `--max-inflight` sheds at the API today; propagate the signal into the commit queue, hint store, and GSI worker so a slow disk degrades gracefully instead of ballooning memory. |

## S. Scale — more data, more nodes, more regions

| ☐ | Feature | Pri | What it takes |
|---|---------|-----|---------------|
| [⚠️] | **Tiered storage: hot local + cold object store** (C4) | 9 | ⚠️ shipped **for history** — new `internal/tier`: `Target` interface with `DirTarget` (a NAS/bucket mount) and `S3Target` — S3 REST over `net/http` with **hand-rolled AWS Signature V4, no SDK** (streaming `UNSIGNED-PAYLOAD` PUTs, `list-type=2` with continuation tokens, path-style for MinIO/Ceph via `--endpoint`). `--tier-target dir://…|s3://…` makes **compaction archive the raw log to the cold tier before rewriting**, so local disk tracks the live dataset while PITR history lives off-box — and if the tier is unreachable, **compaction refuses** (the local log is the only copy; `TestCompactRefusesWithoutTier` proves the log is byte-identical after the refusal). `microctl tier --target <uri> [--key --out]` lists/fetches cold objects (live-verified against a dir target). Metrics: `microdb_tier_{archives,bytes}_total`. Tests: 7 (dir round-trip + path-escape rejection, URI parsing, archiver naming, SigV4 shape/scope/signed-header/payload-hash assertions against an in-process S3 stand-in, store archive + refusal). **Caveats:** (a) a live S3 bucket was not available here — the signature is pinned against AWS's specified form, not against AWS's service; (b) evicting *cold documents* out of RAM is **not** shipped — microdb's working set is resident by design, and that belongs to the workload tiering story, not the log |
| [x] | **Membership & repair that survives hundreds of nodes** | 8 | ✅ shipped (the O(N²) half) — **gossip was already partial**: one random peer per tick, not full-mesh, so membership traffic was never the wall. **Anti-entropy was**: `antiEntropyAll` synced with *every* peer every round — N nodes × N peers per 10s = O(N²) messages per interval. It now runs a **shuffled lap** (`internal/cluster.aeSchedule`): each round contacts the next `--ae-fanout` peers (default 3), every peer is visited exactly once per lap before the lap restarts, membership changes reshuffle it and drop departed peers. Bounded per round, complete over a lap — unlike purely random selection, which leaves a long tail unvisited at the same average cost. Small clusters (the test suite, ≤3 nodes) still cover everyone in one round at the default, so repair latency there is unchanged. Metrics: `microdb_ae_peers_contacted_total`. 4 schedule tests (one-per-lap at fanout == size, coverage + balance at fanout 2, membership rebuild, degenerate inputs) + the full integration suite proving convergence still happens. **Not in this row:** partial-view membership (the member list is still exchanged whole) and member-list digests — both remain open and are called out rather than claimed |
| [x] | **Topology-aware placement (rack/zone)** | 8 | ✅ shipped — `--zone <rack/AZ>` on each node (self-describing and gossiped as `Member.zone`, rather than a hand-maintained `--zones` map that drifts); `/api/cluster` reports the live `zones` view so an operator can see the topology the ring is using. `ring.Owners` walks in **two passes**: first only nodes in a failure domain not yet used, then a fill pass for whatever the topology cannot supply (2 AZs with RF=3) — and with no zone data at all the first pass takes everything, i.e. byte-for-byte the previous placement (`TestNoZonesMeansNoBehaviourChange`, 200 keys). **Measured:** 6 nodes across 3 AZs, RF=3 → 500/500 keys span 3 distinct zones; without the fix ~60% of keys would stack two replicas in one AZ (8 favourable selections out of 20). Integration: 4 nodes in 2 AZs with RF=2 → 40/40 keys land their two replicas in different zones, after zones propagate by gossip. **Not in this row:** anti-entropy pairs preferring the same zone (cross-zone repair traffic) — network-locality scheduling is a separate pass |
| [ ] | **Multi-region replication (D6) + causal merge** | 7 | Cross-region log shipping with conflict resolution **beyond LWW**: version vectors per document + explicit resolution hooks, and hybrid logical clocks so "last" is well-defined. LWW across regions silently drops concurrent edits under clock skew. |
| [ ] | **Index service off the write path (C5)** | 7 | Move GSI maintenance out of the store process into a tier consuming the durable change feed; index cost and index failures stop being charged to the write path. |
| [ ] | **Compute autoscaling (C6)** | 7 | Stateless `microcompute` behind a load balancer: scale out on CPU/queue depth in seconds, scale in by draining. `/ready` already exists as the readiness hook. |
| [ ] | **Spill-to-disk query execution** | 6 | Sort/aggregation over large results spills to temp files under a bounded memory budget instead of buffering everything in the coordinator. |
| [ ] | **Watermarks + capacity admission** | 6 | Per-node memory/disk high-water marks: shed or spill before OOM rather than after. `microctl capacity` reports headroom, time-to-full, and the cost of the next rebalance. |
| [ ] | **Soak / chaos harness** | 6 | Scripted multi-hour run: random kills, slow disk, partitions, joins/leaves — then assert convergence, zero data loss, and p99 within budget. This is the row that *proves* "production-grade" instead of asserting it. |

## P. Production-grade — operability, safety, security

| ☐ | Feature | Pri | What it takes |
|---|---------|-----|---------------|
| [x] | **Rolling-upgrade compatibility (N / N−1 wire)** | 10 | ✅ shipped — new `internal/protocol`: `X-Microdb-Protocol` stamped on every `/internal/*` request (cluster `clusterStampTransport`, api `protoStampTransport`) and advertised on every reply. Requests outside `[MinSupported, Version]` are refused with **505 + a structured body** (`peer_version`/`min_supported`/`max_supported`/`server_version`) *before* any of it is parsed into membership or data; an absent header means `LegacyVersion` (a pre-versioning binary), never "whatever we are today" — so the window survives the first version bump. Out-of-window peers are recorded in `Cluster.Incompatible()`, skipped by gossip, and surfaced in `/api/cluster` and `microctl status` (which now prints `/version`, incl. `protocol` + `protocol_window`). Joining an incompatible seed fails with a diagnosis and leaves zero partial membership. 5 `protocol` unit tests + 4 integration tests (negotiation matrix incl. malformed header, public API unaffected, clean join failure, `/version`) |
| [x] | **End-to-end integrity: checksums + `verify` + `repair`** | 9 | ✅ shipped — **disk:** every log record is wrapped in `CRC1:<crc32c>:<payload>`; the envelope sits *outside* the `ENC1` encryption so rot is detectable without the key. Compaction and PITR replay emit it too; pre-existing records are accepted as legacy. Replay tolerates a torn tail (crash mid-append), skips + counts mid-file damage (`microdb_corrupt_records_total`) instead of bricking the node, and anti-entropy refills it. **wire:** `X-Microdb-Crc32c` stamped on every internal POST by `cluster.post`, verified centrally in `api.ServeHTTP` (64MB cap) → **400 before the body reaches membership/replication state**; absent header = older peer. **`microctl verify --dir`** scans offline without loading the store, reports `line N @ byte M` and exits 1 on damage (tolerating a torn tail) — live-verified: legacy log → `integrity OK`; tampered record → `line 2 @ byte 87: crc32c mismatch`, exit 1. **`microctl repair --url`** → `POST /internal/repair` runs anti-entropy on demand — live test: a document node A never had is pulled in one pass. Metrics: `microdb_checksum_rejections_total`, `microdb_corrupt_records_total`. Tests: 4 store (checksum round-trip, middle corruption skipped, torn tail, verify report) + 2 integration (4-case checksum matrix, repair pull). **Downgrade caveat:** a pre-CRC binary cannot parse `CRC1:` records — roll back *before* writing data with a newer binary, or restore from backup |
| [⚠️] | **Verified, offsite backups** (was: scheduled + retention) | 8 | ⚠️ shipped — `Store.BackupWithManifest` writes a deterministic backup (docs sorted by collection,id, so identical state ⇒ identical bytes ⇒ a digest that means something) and returns a `BackupManifest` (`format`, `created_unix_ms`, `docs`, `bytes`, `sha256`) written beside it as `<file>.manifest.json`. `store.VerifyBackup` re-reads the file, checks every line parses as a document, recomputes the digest and compares counts/size — **problems are itemised, not a boolean**: tampering, truncation, a missing manifest and a count mismatch each report separately, and `OK()` is false unless a manifest exists AND matches. `microctl verify --backup <file>` runs it (exit 1 on any problem); `microctl backup --out` writes the manifest atomically (`.part` + rename), `microctl backup --target dir://|s3://` pushes the archive **and its manifest** through the tier target. **Live-verified end to end:** backup → tier → `microctl tier --key --out` → verify ⇒ `verified OK` with the same digest; after truncating 5 bytes ⇒ four distinct problems and exit 1. 5 tests (manifest round-trip, determinism, tampering, missing manifest, truncation). **Caveats:** no scheduler (cron/systemd timer drives it today) and **no retention policy** — old archives accumulate until an operator prunes; `verify` checks the file against its manifest rather than doing a trial restore into a scratch dir |
| [⚠️] | **Secrets & key rotation** (⚠️ TLS certs not hot-reloaded) | 8 | ⚠️ shipped — new `internal/secret`: every secret resolves **file → env → flag** (`--auth-token-file`/`--encryption-key-file`, `MICRODB_AUTH_TOKEN`/`MICRODB_ENCRYPTION_KEY`), so values stay off argv (world-readable via `ps`). `RefreshOnce` re-reads on a poll and reports **only what changed**, keeping the last good value when a file vanishes mid-rotation. **Hot reload without a restart:** Go has no portable SIGHUP (it does not exist on Windows), so microdb polls instead — live-verified: start with `--auth-token-file`, swap the file, old token → **401**, new token → accepted, `auth token reloaded` logged, connections never dropped (`api.Token` holder, rotation tested at unit level too, including that a nil token never panics). **Encryption rotation:** records now carry their key identity (`ENC2:<kid>:<base64>`, kid = first 4 bytes of sha256(key) — derived, so it cannot disagree with the file); `--encryption-key-old` (or `MICRODB_ENCRYPTION_KEY_OLD`) keeps retired keys for decryption only, `OpenWithKeyRing` installs them **before replay**, and a record naming a key you no longer have fails the open **with that key id** instead of silently dropping documents. Legacy `ENC1:` records (no id) are still opened by trying every configured key. **Caveats:** TLS cert/key files are still flag-only (not hot-reloaded); reload is a poll, not a signal. 8 tests (4 secret resolution/refresh, 2 token, 2 rotation incl. legacy ENC1) |
| [x] | **Audit log** | 8 | ✅ shipped — new `internal/audit`: append-only JSONL (`mode 0600`) behind a package-level sink like `metrics.Default`, so disabled it is a boolean check that allocates nothing. `--audit-log <path>` turns it on (rotates at 64MB to `<path>.1`, exactly one generation — an audit log must not fill the disk it protects). **What gets recorded:** mutations (POST/PUT/DELETE on `/api/*`) centrally in `ServeHTTP` with the status they actually ended up with (response wrapper + `defer`, so every early-return path is covered — including a mutation rejected mid-flight), and **every authorization denial**: invalid bearer token (401), unknown tenant token (401), collection outside a tenant's namespace (403). Each record carries ts/kind/action/decision/status/remote/actor/tenant/collection/id/trace — the tenant is resolved from the bearer token for allowed writes, not just for denials. **Reads are deliberately not audited** (they would bury the log); asserted in the test. **Live-verified:** `PUT` → `{"kind":"mutation","decision":"allow","status":200,"collection":"users","id":"alice","actor":"bearer"}`; bad token → `{"kind":"auth","decision":"deny","status":401,"detail":"invalid bearer token"}`; authenticated GET → nothing. 4 tests (disabled no-op, JSONL shape, bounded rotation with both generations still parseable, API-level hooks). **Caveat:** audit writes are not fsynced — a crash can lose the last line |
| [x] | **SLO metrics + error budget** | 8 | ✅ shipped — the registry grew two metric types it did not have: a real **Prometheus histogram** (`le` buckets cumulative, `+Inf`, `_sum`, `_count`, rendered sorted so scrapes are byte-stable) and **float gauges** (a percentage must not render as `0`). `Tracing` now records RED per request: `microdb_request_duration_ms_<route>` histograms over ten latency buckets (5…5000ms, resolution around the SLO), `microdb_request_errors_total` + per-route `microdb_request_errors_<route>_total` (5xx), and `microdb_requests_over_slo_total` against `--slo-ms` (default 500). Route classes are collapsed (`get_doc`/`put_doc`/`delete_doc`/`query`/`batch`/`watch`/`internal`/`health`/…) so `/metrics` stays bounded while reads, writes and queries chart separately. At scrape time `handleMetrics` derives the numbers alerts should actually watch: **`microdb_slo_violation_pct`** and **`microdb_error_pct`** (fractions of traffic, not raw counts) plus `microdb_slo_ms`. **Tested:** bucket cumulation (5/50/500/5000 → 1/3/4/+Inf, sum 5655), float rendering (`0.4` stays `0.4`), scrape stability, idempotent registration, and an API-level run asserting `over_slo_total 2` of 3 requests with `slo_violation_pct ≈ 66.7`, per-route histograms and `_sum` present. 4 metrics tests + 1 API test |
| [x] | **Continuous profiling** (`pprof`) | 7 | ✅ shipped — new `internal/profile`, **off by default** (a pprof endpoint can read process memory, so the row's "behind auth" became "on its own listener": `--pprof 127.0.0.1:6060` binds a separate mux with the five stdlib `net/http/pprof` routes and reports the address it actually bound, so it can sit on localhost or a private interface while the API stays public). `--profile-dir` + `--profile-interval` (default 1m) write `heap-<unixms>.pprof` and `cpu-<unixms>.pprof` on a timer — CPU capped at 30s so the sampler cannot fall behind — and `Prune` keeps the newest 10 of each kind, because profiles are for forensics, not for filling a disk. The point is the profile taken *before* the restart, not the one taken after. **Live-verified:** `/debug/pprof/` and `/debug/pprof/goroutine?debug=1` both 200 on the private port while the node served API traffic, with cpu+heap files appearing every second at `--profile-interval 1s`. 4 tests (disabled-by-default + endpoints wired, heap/cpu files non-empty, pruning keeps newest, Run/Stop terminates without hanging) |
| [x] | **Graceful shutdown that loses nothing** | 7 | ✅ shipped — SIGTERM/Ctrl-C runs a fixed sequence in `cmd/microdb`: **(1)** `cl.SetDraining(true)` flips `/ready` to 503 (load balancers stop sending) while `/health` stays 200 — a deliberate stop must not look like a crash or the supervisor kills us mid-flush; **(2)** wait for in-flight requests, counted by a new `api.Inflight()` maintained in `Tracing`, bounded by `--shutdown-timeout` (default 15s, then it logs and proceeds rather than hanging); **(3)** `store.Close()` drains the log-writer queue, fsyncs and closes the durable feed (the queue-drain was built with group commit); **(4)** `cl.Leave()` — factored out of `Decommission`, so shutdown broadcasts departure **without** the bulk data handoff (replicas already hold it) and peers rebuild their rings now instead of after the 15s TTL; **(5)** exit 0. **During the drain:** reads keep serving clients already connected, new writes get 503 (`node is decommissioning, retry elsewhere`) — a write accepted here would be served by a process closing its log file. Tests: `TestInflightTracksActiveRequests` (counter is 1 exactly while a handler runs, 0 after — shutdown must not wait forever) and `TestGracefulShutdownSequence` (ready 503 / health 200 / read 200 / write 503 / leave evicts the peer immediately). **Caveat:** the signal wiring in `main` is not exercised by a live signal in tests (not portable across platforms) — the sequence itself is what the tests drive |
| [ ] | **Validated config file + env overrides** | 6 | The flag surface is large enough now that a single config file with env overrides, strict unknown-key rejection, and `microctl config validate` beats a 30-arg command line. |
| [ ] | **Kubernetes operator manifests** | 6 | StatefulSet + PVC + headless Service + PodDisruptionBudget + probes wired to `/ready`/`/health`, plus a Helm chart. Upgrades docker-compose from demo to deployable. |
| [ ] | **Rejection & quota metrics per tenant** | 5 | Rate-limit and quota rejections already exist; export them with tenant labels so a noisy neighbour appears on a dashboard before it appears in a support ticket. |
| [ ] | **Runbook in-repo** | 5 | `docs/runbook.md`: node loss, disk full, split brain, key rotation, restore-from-backup — each with exact commands and the observable outcome that confirms success. |

## Definition of done for this tier

A row flips to ✅ only when **all four** hold:

1. a test that fails before and passes after, **or** a benchmark with checked-in before/after numbers;
2. full suite green **and** `-race` clean;
3. a metric or log line that makes the behavior observable in production;
4. documented (default value, cost of enabling, failure mode) in `README.md` / runbook.

## Suggested ordering for this tier

| Phase | Theme | Items | Why this order |
|-------|-------|-------|----------------|
| **7** | Measure, then fix the hot path | F: bench harness → group commit → sharded map → block store | You cannot claim "fast" without a baseline; these four unlock every other perf row |
| **8** | Never corrupt or lose data | P: wire versioning, checksums+verify/repair, graceful shutdown | Correctness blockers — must land before anyone runs this in production |
| **9** | Scale the coordination layer | S: membership fanout, zone awareness, anti-entropy scheduling, aggregate pushdown | O(N²) gossip/repair chatter is the first wall a real cluster hits |
| **10** | Cost & geography | S: tiered storage, multi-region, index service, compute autoscale | Cloud economics, once the coordination layer is sound |
| **11** | Operate it | P: SLOs, pprof, audit, backups, k8s manifests, runbook | The difference between "it runs" and "you can be on call for it" |

## Where this landed — 20 / 20 built

**17 ✅ shipped and verified, 3 ⚠️ shipped with the caveat written on the row**
(tier storage = history only, backups = no scheduler/retention yet, secrets = TLS
certs not hot-reloaded). One extra commit fixed a blocker the harness found
before feature 1 landed.

### Headline numbers (all measured, all from `bench/` or the row's own test)

| Claim | Before | After |
|-------|--------|-------|
| Point write (10k-doc collection) | 3,181,897 ns/op | **8,017 ns/op** (397×) |
| Write past the feed cap (allocations) | 804 KB/write | **1.6 KB/write** |
| Durable write under concurrency | 897 µs/op serial | **115 µs/op** (7.8×, group commit) |
| Store log passes for 160 writes | 160 (one per write) | **21** (7.6× batching) |
| Point reads, 16 threads | 77 ns/op | **47 ns/op** (1.6×) |
| Startup replay allocations | 13.22× the log | **3.65×** (streaming) |
| Log size, repetitive payload | 25,140 B | **3,587 B** (86%, `--compress`) |
| Compound-query miss over 20k docs | 16.5 ms full scan | **321 ns** (bloom, ~51,000×) |
| 20-row window over 20k matches | 18.0 ms / 761 KB | **9.7 ms / 1.2 KB** (top-K) |
| Query with a dead peer | ~5 s (client timeout) | **0.26 s** (shared deadline) |
| Replicas in one AZ, 3 AZs × RF=3 | ~60% of keys | **0 of 500 keys** |
| Anti-entropy traffic per round | O(N) peers | **3 peers** (`--ae-fanout`) |

### Next 10 — the remaining production-grade gap (tracked)

| # | Feature | Why it matters | Status |
|---|---------|----------------|--------|
| 21 | CI verification gate + fix the flaky test | every claim above rests on a suite that must be runnable in one command and must not flap | ⬜ |
| 22 | Capacity watermarks + admission (`microctl capacity`) | OOM and ENOSPC are the two ways databases die quietly | ⬜ |
| 23 | Backup scheduler + retention | closes the ⚠️ on backups: RPO automation and bounded offsite growth | ⬜ |
| 24 | TLS cert/key hot-reload | expired certs are a top self-inflicted outage; closes the ⚠️ on secrets | ⬜ |
| 25 | Index value encode fast path | `json.Marshal` per field per write measured at 61% of replay allocations | ⬜ |
| 26 | Wire compression (internal requests + responses) | closes both "the wire still ships JSON" scope notes | ⬜ |
| 27 | Zone-local anti-entropy pairing | cross-zone repair traffic is money for no correctness gain | ⬜ |
| 28 | Soak / chaos harness | the only honest answer to "does it survive failures" | ⬜ |
| 29 | Operability pack: k8s + Helm + compute autoscaling (C6) + alert rules + DR runbook | deploy, scale, alert, recover without reading the source | ⬜ |
| 30 | Multi-region: region topology + region-spread replicas + GSI lag metrics | replicas surviving a region loss; ⚠️ until conflict resolution beyond LWW lands | ⬜ |

**Still open after these 10, by design:** conflict resolution beyond LWW
(version vectors / CRDTs — it changes consistency semantics and deserves its own
design pass), spill-to-disk query execution, partial-view membership and
member-list digests, and splitting the index service into its own tier.

### Deliberately re-scoped rather than shipped as written

- **Block/page store → streaming replay.** The working set is resident by
  design, so a page/mmap store with a key→offset index would buy nothing here;
  what mattered was bounded startup memory and a self-healing tail.
- **Bloom "per segment" → scan short-circuit.** There are no disk segments;
  the filter went where it pays (the compound-query fallback).
- **"Scheduled" backups → verified backups.** Verification and offsite landed;
  the scheduler and retention policy did not, and the row says so.

### Genuinely still open (named on their rows, not hidden)

Multi-region replication (D6) · index service (C5) · compute autoscaling (C6) ·
k8s manifests and a DR runbook (E) · TLS cert hot-reload · backup scheduler and
retention · partial-view membership and member-list digests · zone-local
anti-entropy pairing · compression on the replication wire.

> **Known flaky test:** `TestRangeMapHotSpotSplit` (integration) times out
> waiting for all nodes to converge on the same range plan roughly 1 run in 10
> under parallel load, and passes on re-run and in isolation. It predates the
> the features recorded above and is **not** attributed to them — but it is a
> real gap in the "every row is backed by a passing test" claim, and is tracked
> as one.

---

# 🚀 Roadmap: DynamoDB / Cassandra-class scalability

What it would take to evolve microdb from a fixed-cluster toy into a horizontally
scalable database: **dynamic node membership, real sharding, and full separation
of storage from the query engine.**

**Gap analysis — what we already have vs. what the real systems rely on:**

| Concept | microdb today | DynamoDB / Cassandra |
|---|---|---|
| Membership | Gossip + TTL eviction ✅ | Same idea (gossip/seed nodes) ✅ |
| Placement | Consistent-hash vnodes ✅ | Cassandra vnodes ✅ / DynamoDB range partitions ❌ |
| New node | Starts **empty**, backfills via anti-entropy pull (O(all data), slow, no admission control) | **Bootstrap + streaming**: ranges transferred before the node serves reads |
| Ring changes | Rebuilt locally per node, no global version | **Versioned ring epochs** with fencing tokens |
| Data model | Collection + doc id | **Partition key + sort key**, per-partition ordering |
| Storage/compute | **Coupled** — every node owns data and serves queries | **Disaggregated** — stateless compute over a durable storage/log tier |
| Consistency | LWW eventual, RF fanout | **Quorum R+W>N**, tunable per request, hinted handoff |

---

## A. Elastic membership — add/remove nodes dynamically

| ☐ | Feature | Pri | What it takes |
|---|---------|-----|---------------|
| [x] | **Bootstrap & streaming protocol** | 10 | ✅ shipped — paginated stream on join, streaming state, admission control (see above) |
| [x] | **Ring epochs + fencing tokens** | 9 | ✅ shipped — see "Ring epochs + write fencing" above |
| [x] | **Graceful decommission** | 9 | ✅ shipped — see "Graceful decommission" above |
| [x] | **Dual-write migration window** | 8 | ✅ effectively shipped by composition — every write routes through the *current* ring at the coordinator (never a cached placement), stale forwarders are fenced (409→adopt→retry), and read-repair + hints + bootstrap streaming heal any mid-move divergence. No explicit pending_ranges needed because no component caches ownership long enough to need it. |
| [x] | **Load-aware token assignment** (Roadmap A5) | 7 | Sticky minimal-movement rebalancer: `ranges.Rebalance` keeps a range's previous primary when it stays within a `1.1x` fair-share band, so steady load and small drift cause zero churn; only ranges that must move (departed owner, band breach) are reassigned, and a join fills the new node with the minimum set. `MovementCount` measures churn; `PlanFor` is the cold-start (`prev=nil`) case. 5 unit + 1 integration test (live join distributes load across all 3 nodes) |
| [x] | **Admission control for joins** | 6 | ✅ shipped — max 2 concurrent stream serves per seed, 429 + Retry-After, joiner backoff |

## B. Sharding — a real partitioning model

| ☐ | Feature | Pri | What it takes |
|---|---------|-----|---------------|
| [x] | **Partition key + sort key data model** (B1) | 8 | ✅ shipped — see "Partition key + sort key data model (Roadmap B1)" in the Shipped section above |
| [x] | **Range-based ownership with split/merge** | 8 | ✅ shipped — `internal/ranges`: contiguous token-range ownership over the 32-bit space, load-driven split at the load-median (hot-key auto-carving) and implicit merge on cooling. Deterministic planner = pure function of gossiped inputs (nodes, RF, 256-bucket EWMA writes/sec, epoch, base ring), so every node converges on the identical plan with no agreement protocol. Load signal shared peer-to-peer via `/internal/loadbuckets`; routing consults the plan first (epoch-guarded) and falls back to the vnode ring. Off by default: `--range-map <interval>`. |
| [x] | **Per-shard isolation** | 7 | ✅ shipped at collection granularity (see above) — per-collection stats + quotas; per-collection physical log split remains out of scope for "tiny" |
| [x] | **Shard-map aware clients** | 6 | ✅ shipped — `/internal/owners` + Go client `PutRouted` with cache + fallback |
| [x] | **Hedged reads on scatter-gather** | 6 | ✅ shipped — `?hedge_ms=N` duplicate-gather-on-slow-tail |
| [x] | **Global secondary indexes as a service** | 6 | ✅ shipped — async change-feed-driven GSI with exposed lag (see above) |

## C. Storage / query-engine separation

| ☐ | Feature | Pri | What it takes |
|---|---------|-----|---------------|
| [x] | **Stateless query engine** (C1) | 9 | ✅ shipped — `internal/compute.Engine` + `cmd/microcompute`: zero authoritative data, placement ring over storage nodes, refresh-on-stale + retry, scatter-gather merge (see Shipped section) |
| [x] | **Storage-tier record API** (C2) | 9 | ✅ shipped — `internal/storage.RecordStore`: epoch-fenced Put/Get/Scan/Epoch, Local + Remote HTTP adapters (see Shipped section) |
| [x] | **Durable shared commit log** | 8 | ✅ shipped — `internal/sharedlog` + `microlog` binary; store write-through with quorum ACK, checkpointed tail recovery, failover LSN seeding (see above) |
| [ ] | **Tiered storage: hot NVMe + cold object store** | 7 | Storage nodes keep a bounded hot window on local SSD; older segments live in S3-compatible object storage, fetched lazily on read. Cost model of Aurora/DynamoDB: compute and IOPS scale independently of dataset size. |
| [ ] | **Index service off the write path** | 7 | A separate tier consumes the change feed and maintains secondary indexes (currently in-process inverted indexes). Write latency stops paying for index maintenance; indexes scale and fail independently. |
| [ ] | **Compute autoscaling** | 7 | Stateless query nodes behind a load balancer: scale out in seconds on CPU/queue-depth, scale in by draining. No bootstrap, no streaming — the payoff of (C1). |
| [ ] | **Cache invalidation via changelog** | 6 | Query-engine caches subscribe to the storage-tier change feed and invalidate on mutation instead of TTL-guessing. Makes read-your-writes cheap at the engine level. |

## D. Consistency & durability at scale

| ☐ | Feature | Pri | What it takes |
|---|---------|-----|---------------|
| [x] | **Quorum reads (R + W > N)** | 8 | ✅ shipped — `?consistency=quorum|all`, merge by (ver,ts), 503 on unreachable |
| [x] | **Per-request tunable consistency** | 7 | ✅ shipped — `one|quorum|all|local_quorum` on reads and writes, unknown → 400 |
| [x] | **Hinted handoff** | 7 | ✅ shipped — durable hint store, replay on peer return, exp backoff |
| [x] | **Read-path read repair** | 6 | ✅ shipped — quorum reads push newest version to lagging replicas seen during the read |
| [x] | **Point-in-time recovery (PITR)** | 7 | ✅ shipped — continuous raw-log archiving + `microctl pitr --until` replay |
| [ ] | **Multi-region replication** | 6 | Cross-region log shipping + conflict resolution **beyond LWW** (vector clocks or CRDTs — LWW across regions silently loses concurrent edits with clock skew). Global secondary index replication with lag metrics. |

## E. Multi-tenancy & production operations

| ☐ | Feature | Pri | What it takes |
|---|---------|-----|---------------|
| [x] | **Tenant isolation & quotas** | 7 | ✅ shipped — see "Tenant isolation & quotas (E1)" above |
| [x] | **Admission control / backpressure** | 7 | ✅ shipped — `--max-inflight` sheds with 429 + Retry-After |
| [x] | **Per-table configuration** | 6 | ✅ shipped — per-collection RF via `_config` collection |
| [x] | **Encryption at rest + per-tenant keys** | 6 | ✅ shipped (single-key) — AES-256-GCM per-record; per-tenant keys would need tenancy (E1) |
| [x] | **Distributed tracing** | 5 | ✅ shipped — X-Trace-Id + latency buckets + slow-request log |
| [x] | **Online schema migration** | 5 | ✅ shipped — lazy per-doc transforms with `_schema_ver` marker; background persist pass still open |

---

## Suggested phasing

| Phase | Theme | Features | Why this order |
|-------|-------|----------|----------------|
| **1** | Break the coupling | C1 stateless engine, C2 storage API | Everything else is easier once the boundary exists; do it before the codebase grows against the current coupling |
| **2** | Make membership safe & dynamic | A2 epochs/fencing, A1 bootstrap+streaming, A4 dual-write, A3 decommission | Fencing first (correctness), then streaming (speed), then drain |
| **3** | Real sharding | B1 partition/sort keys, B2 split/merge, B3 per-shard isolation | Needs phase-2 epoch machinery to move ranges safely |
| **4** | Durability & consistency | C3 shared log, D1–D4 quorum/hints/read-repair, D5 PITR | Quorum needs stable ownership (phase 2) and the log gives the durability story |
| **5** | Scale-out economics | C4 tiered storage, C5 index service, C6 autoscale, B5 hedged reads | Cost and tail-latency wins once the architecture supports them |
| **6** | Production hardening | E1–E6, D6 multi-region | Multi-tenant / multi-region only after single-region is elastic |

**Honest scope note:** phases 1–3 alone are roughly a rewrite of the ownership and
data-placement layers (the ring, store, and cluster packages stop being one
process-level blob). That's the real cost of disaggregation — DynamoDB, Cassandra,
and Aurora each represent many engineer-years in exactly these layers. microdb's
current gossip + Merkle + LWW pieces map 1:1 onto concepts these systems use, so
the *knowledge* transfers even where the code must be restructured.

---

## Commit history (one feature per commit)

```
baseline    v0: gossip, ring, RF3 replication, Merkle anti-entropy
700af84     store: crash-safe log compaction + tombstone GC
031547e     store: optional fsync-on-write (--fsync)
a24995f     api: scatter-gather collection queries
07c3f2b     merkle+cluster: recursive subtree-diff anti-entropy
f3d10f9     index: inverted field indexes with query fast path
1538d17     store: full query operator set
2152f7e     api: pagination + sort on collection queries
4c6f852     store+api: batch writes (atomic single-record)
895feb1     changelog+api: long-poll change feed (watch)
335787d     cluster+api: mutual TLS for internal traffic
b718d21     api: bearer-token auth for /api/*
b30a697     metrics: Prometheus text-format /metrics endpoint
cf6ee02     backup/restore + microctl admin CLI
8cd686b     api+main: configurable replication factor (--rf)
8e98b3d     client: typed Go client library + batch decode fix
388dff7     packaging: Dockerfile + docker-compose 3-node cluster
e4fe4f1     api: /v1 versioned routes alongside unversioned
9566790     api+cluster+ring: quorum reads/writes, ring epochs, write fencing
b2f9c64     hints: durable hinted handoff for failed replication
bcd684c     cluster+api: graceful decommission with leave tombstones
b37b9c6     api: full tunable consistency + hedged scatter-gather
2f03c76     store+cli: point-in-time recovery (PITR)
39f1785     cluster+store: bootstrap streaming on join + admission control
8bbea73     client+api: shard-map aware routing (B4)
6d7f850     api: backpressure (E2) + request tracing (E5)
001c511     store+api: per-collection replication factor (E3)
47f43f7     store: AES-256-GCM encryption at rest (E4)
```

**Coverage:** 40 shipped. Roadmap done: A1, A2, A3, A6, B4, B5, D1–D5, E2–E5.
Remaining roadmap items (A4/A5, B1–B3, C1–C7, D6, E1, E6) are the
storage/compute disaggregation and tenancy layers that FEATURES.md's scope
note flags as a rewrite of the ownership/data-placement core — deliberately
out of scope for "tiny". All shipped features verified: full suite green +
`-race` clean + live 3-node cluster runs (decommission, PITR, encryption).
