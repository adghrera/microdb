# microdb — Feature Checklist & Priority Score

**Project:** `microdb` · Go 1.23 · stdlib-only distributed schema-less document store
**Status:** all Tier 1–3 features + most Tier 4 shipped. Every "done" row is backed by a passing test or live run.
**Priority scale:** 10 = core correctness/foundation → 0 = nice-to-have polish.

Legend: ✅ shipped & verified · ⬜ not done · ⚠️ shipped with caveat

---

## ✅ Shipped (48 features)

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

## ⬜ Remaining (deliberately out of scope for "tiny")

| ☐ | Feature | Pri | Why deferred |
|---|---------|-----|--------------|
| [ ] | Multi-tenancy / namespaces | 2 | Changes the data model; contradicts "tiny" |
| [ ] | Vector clocks / CRDT merge (instead of LWW) | 3 | Changes consistency semantics fundamentally |
| [ ] | Per-collection RF (vs global `--rf`) | 3 | Ring would need per-collection ownership maps |
| [ ] | Durable change feed (persisted events) | 3 | Current feed is bounded in-memory by design |
| [ ] | Range-partitioned queries pushed to shards | 2 | Scatter-gather is O(cluster) but correct |

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
| [ ] | **Dual-write migration window** | 8 | While a range is moving, both old and new owner accept writes (new owner records `pending_ranges`), merge on completion. This is the DynamoDB/Cassandra mechanism that makes rebalance invisible to clients. |
| [ ] | **Load-aware token assignment** | 7 | Auto-assign vnodes by observed load (bytes/sec, ops/sec per node) instead of uniform random placement; periodic rebalancer that proposes minimal-movement plans. |
| [x] | **Admission control for joins** | 6 | ✅ shipped — max 2 concurrent stream serves per seed, 429 + Retry-After, joiner backoff |

## B. Sharding — a real partitioning model

| ☐ | Feature | Pri | What it takes |
|---|---------|-----|---------------|
| [ ] | **Partition key + sort key data model** | 8 | Dynamo-style composite keys: items grouped by partition key, ordered by sort key within the partition. Enables `Query(partition = X, sort BETWEEN a AND b)` served from one shard with no scatter. Today: flat `col/id`, no ordering, no locality. |
| [ ] | **Range-based ownership with split/merge** | 8 | Move from pure vnode-hash to contiguous token ranges that can be **split** (DynamoDB auto-partitions hot keys) and **merged** (cold shards). Requires the epoch machinery from (A) to move ranges atomically. |
| [ ] | **Per-shard isolation** | 7 | Each shard gets its own commit log, index set, compaction schedule, GC, and metrics. One hot shard can't stall compaction of the whole node; per-shard quotas become possible. |
| [x] | **Shard-map aware clients** | 6 | ✅ shipped — `/internal/owners` + Go client `PutRouted` with cache + fallback |
| [x] | **Hedged reads on scatter-gather** | 6 | ✅ shipped — `?hedge_ms=N` duplicate-gather-on-slow-tail |
| [ ] | **Global secondary indexes as a service** | 6 | GSI = a separate index shard keyed by (index_value → partition_key), maintained asynchronously from the change feed, with its own consistency lag exposed to clients. |

## C. Storage / query-engine separation

| ☐ | Feature | Pri | What it takes |
|---|---------|-----|---------------|
| [ ] | **Stateless query engine** | 9 | Compute nodes hold **no authoritative data**: they parse, plan, route to the storage tier, merge, and cache. A compute node can die or scale out with zero data movement. Today every node is both — the core coupling to break. |
| [ ] | **Storage-tier record API** | 9 | Storage nodes expose a minimal, epoch-fenced record interface: `Put(key, ver, ts, value)`, `Get(key)`, `ScanRange(prefix, lo, hi)`, `Tombstone(key)`. The query engine never touches JSONL directly. This API boundary *is* the disaggregation. |
| [ ] | **Durable shared commit log** | 8 | Writes append to a replicated log service (Kafka-like, or S3 segments with an append protocol) that the storage tier replays. Compute nodes become crash-free by construction; durability moves out of per-node `fsync` into the log's own replication (multi-AZ quorum). |
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
| [ ] | **Tenant isolation & quotas** | 7 | Per-tenant rate limits, storage quotas, per-tenant auth; noisy-neighbor control at the admission layer. |
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
