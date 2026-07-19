# kvdb

A small **networked key-value database** written in Rust, exposed over an
**HTTP/REST API** with HTTP Basic authentication. It's a learning project that
mirrors the starting architecture of LSM-tree engines (RocksDB, LevelDB): data
lives in memory and is durably journaled to disk through a write-ahead log
(WAL), which is replayed on startup to rebuild state.

## Architecture

```
src/
├── lib.rs        — module exports
├── store.rs      — storage engine: memtable (BTreeMap) + WAL + recovery
├── http.rs       — axum router, REST handlers, Basic-auth middleware
└── bin/
    ├── server.rs — HTTP server (axum + tokio)
    └── client.rs — HTTP client: interactive REPL and one-shot mode
tests/
├── store.rs      — storage engine integration tests
└── http.rs       — HTTP router tests (auth, CRUD) via tower oneshot
```

**Durability guarantee:** every mutation is first appended to the WAL and flushed
to disk, and only then applied to the memtable. A record left torn by a crash
mid-write is treated as uncommitted and dropped during recovery.

## REST API

| Method | Path             | Auth | Behavior                              |
|--------|------------------|------|---------------------------------------|
| GET    | `/health`        | no   | `200 "PONG"` — liveness probe / PING  |
| GET    | `/v1/keys/{key}` | yes  | `200` body = value, or `404`          |
| PUT    | `/v1/keys/{key}` | yes  | request body = value → `200 "OK"`     |
| DELETE | `/v1/keys/{key}` | yes  | `200 "OK"`, or `404` if key was absent|

Requests to `/v1/*` without valid credentials get `401 Unauthorized` with a
`WWW-Authenticate: Basic` challenge.

## Authentication

Credentials are read from the environment and are **required** — the server
refuses to start without them:

```sh
export KVDB_USER=admin
export KVDB_PASSWORD=secret
```

Clients authenticate with HTTP Basic auth (`curl -u user:pass`, or the
`Authorization: Basic ...` header).

## Build

```sh
cargo build --release
cargo test           # fast suite: storage engine + HTTP router tests

# Heavier suites are opt-in (kept out of the default run):
cargo test --release --test load -- --ignored --nocapture --test-threads=1
cargo test --release --test perf -- --ignored --nocapture --test-threads=1
```

GitHub Actions runs formatting, Clippy, the fast suite, sequential release load
tests, informational benchmarks, and a Docker persistence smoke test on every
push to `master` and pull request. Load/performance logs are retained as build
artifacts for 14 days; benchmark timings appear in the run summary but do not
gate the build because shared-runner performance is noisy. The workflow can
also be started manually with `workflow_dispatch`.

## Load tests and performance baseline

The ignored suites have different jobs. `tests/load.rs` asserts correctness
under volume, concurrency, repeated reopen, compaction, and MVCC retention;
timing never decides whether these tests pass. `tests/perf.rs` is an
informational, dependency-free benchmark harness. Run both in release mode with
one test thread as shown above so workloads do not compete with each other.

Current load suite (all passed in `17.73 s` on the machine described below):

| Scenario | Workload | What is verified |
|---|---:|---|
| Bulk persistence | 100k SET + 25k DELETE, ~100 flushes | Every value/tombstone after reopen with 90+ SSTables |
| Concurrent HTTP | 200 distinct-key writers + 200 hot-key writers | No lost/corrupt writes; valid last-writer-wins result |
| Deterministic mixed | 100k ops, 10k keys, 60% SET / 25% DELETE / 15% GET | Exact match with `BTreeMap` across periodic flush, compaction, and 10 reopens |
| MVCC + retention | 30k commits over 2k keys + 4k historical probes | Historical reads before GC and retained reads after GC/reopen |

### Local benchmark results

Measured on 2026-07-18 under WSL2 (Linux 6.6), Intel Core i5-7200U
(2 cores / 4 threads), 9.7 GiB RAM, Rust 1.97.1, release `opt-level=3`, with
temporary files on the WSL filesystem. Values are the median of three complete
sequential runs. Latency is total elapsed time divided by operation count; it
is a mean, not a p95/p99. Setup/population is outside the measured interval.

| Operation | Dataset | Median throughput | Mean latency |
|---|---:|---:|---:|
| SET, individual WAL record + flush | 200k | 180,815 ops/s | 5.531 us/op |
| SET, atomic batches of 100 | 200k | 190,257 ops/s | 5.256 us/op |
| DELETE, individual tombstone | 50k | 154,997 ops/s | 6.452 us/op |
| GET, memtable hit | 100k | 1,869,809 ops/s | 0.535 us/op |
| GET, one SSTable hit | 100k | 36,333 ops/s | 27.523 us/op |
| GET miss, 10 Bloom checks | 100k | 245,046 ops/s | 4.081 us/op |
| Historical `get_at`, 5-version SSTable | 20k | 14,424 ops/s | 69.330 us/op |
| Flush memtable to one SSTable | 200k records | 610,572 records/s | 1.638 us/record |
| Full compaction, 10 SSTables to 1 | 100k records | 344,617 records/s | 2.902 us/record |
| Retention GC | 100k input versions | 720,612 versions/s | 1.388 us/version |
| WAL recovery | 200k records | 855,672 records/s | 1.169 us/record |
| Open one 200k-key SSTable | 1 table | - | 57.611 ms total |
| Materialize copy-on-snapshot | 100k keys | 25,697 keys/s | 38.915 us/key |
| HTTP GET through Axum router, no TCP | 50k requests | 181,014 req/s | 5.524 us/req |
| HTTP PUT through Axum router, no TCP | 20k requests | 48,736 req/s | 20.519 us/req |

The HTTP rows include routing, Basic auth, body handling, mutex locking, and the
Store operation, but deliberately exclude socket/network overhead. WAL `flush`
means flushing Rust's buffered writer to the OS; kvdb does not currently call
`sync_data` for every mutation, so these numbers must not be read as fsync-level
durability latency.

Disk footprint from the same runs:

| State | Bytes | Normalized size |
|---|---:|---:|
| WAL, 200k individual SET records | 9,488,890 | 47.4 B/record |
| WAL, 200k SETs in batches of 100 | 7,914,890 | 39.6 B/record |
| One 200k-key SSTable store | 10,651,487 | 53.3 B/record |
| 100k distinct records before / after full compaction | 5,271,088 / 5,270,251 | 52.7 B/record |
| 100k MVCC versions before / after retention | 4,926,142 / 1,534,140 | 68.9% reduction |

These figures are a regression baseline for this machine, not an SLA. WSL2 I/O
showed substantial run-to-run variance, which is why the table reports medians
and keeps the exact reproduction command beside the results.

## Run

Server (defaults to `0.0.0.0:6380`, WAL at `kvdb.wal`):

```sh
KVDB_USER=admin KVDB_PASSWORD=secret cargo run --bin kvdb-server
# optional: kvdb-server <BIND_ADDR> <WAL_PATH>
```

### Logging

The server logs to **stderr** and, if `KVDB_LOG_FILE` is set, also appends to
that file. The minimum level is controlled by `KVDB_LOG` (`error`, `warn`,
`info` (default), `debug`):

```sh
KVDB_LOG=debug KVDB_LOG_FILE=kvdb.log \
KVDB_USER=admin KVDB_PASSWORD=secret cargo run --bin kvdb-server
# [2026-07-01T12:00:00Z INFO  kvdb::server] listening on 0.0.0.0:6380
```

### Storage layout

Alongside the WAL (`kvdb.wal`), a full store keeps sorted **SSTable** files
(`kvdb-000001.sst`, …) and a **manifest** (`kvdb.manifest`) that lists them in
order together with the latest durable commit sequence and retained-history
boundary. When the memtable grows past `KVDB_MEMTABLE_LIMIT` entries (default
`1024`) it is flushed to a new SSTable, the manifest is updated, and the WAL is
truncated. Each key record
contains its versions in ascending commit-sequence order. SSTables group those
records into 64-entry blocks with a persisted
**sparse index** (one first-key + byte range per block) and a **Bloom filter**.
Opening a table does not load every key into memory; a Bloom negative skips the
file entirely, while a possible hit binary-searches the index and scans one
block. Once `KVDB_COMPACTION_THRESHOLD` SSTables accumulate (default `8`), the
Store automatically performs a full compaction: it deduplicates identical
commit sequences, preserves historical values and tombstones for MVCC, and
atomically replaces the manifest. Setting the threshold to `0` disables
automation; `Store::compact()` remains available for an explicit run.
Automatic and ordinary manual compaction preserve every currently retained
version.

### Atomic batches

Every standalone `SET`/`DELETE` receives a monotonically increasing commit
sequence. Library users can group mutations into a `WriteBatch`; the whole
batch is encoded as one WAL record and consumes one sequence number, so recovery
applies either every operation or none of them if the trailing record is torn:

```rust
use kvdb::{Store, WriteBatch};

let mut store = Store::open("kvdb.wal")?;
let mut batch = WriteBatch::new();
batch
    .set(b"user:1".to_vec(), b"Alice".to_vec())
    .delete(b"user:old".to_vec());
let sequence = store.write_batch(batch)?;
# Ok::<(), std::io::Error>(())
```

`Store::snapshot()` returns an immutable read-only copy of all currently visible
values together with its commit sequence. `snapshot_at(sequence)` reconstructs
the state at an older commit. Later writes, flushes, and compactions do not
affect either snapshot. Creating one costs O(visible keys + values) memory; the
underlying memtable and SSTables retain ordered per-key versions for historical
reads.

### History retention

History is retained indefinitely by default. To reclaim old versions, call
`compact_with_retention(history_start)`: it flushes pending writes, fully merges
the SSTables, and retains exact reads and snapshots from `history_start` onward.
The newest value at or before that sequence is kept as an anchor for each key;
an obsolete tombstone is removed once no older SSTable can resurface behind it.
The boundary is persisted in the manifest and can only advance. Historical
`get_at` and `snapshot_at` calls before the boundary return `InvalidInput`
rather than a partial answer.

```rust
store.compact_with_retention(10_000)?;
assert_eq!(store.history_start_sequence(), 10_000);
```

`Store::begin_transaction()` builds on that snapshot with a private write
overlay. Reads see the transaction's own SET/DELETE operations first. Commit is
optimistic: `commit_transaction()` writes the overlay as one atomic batch only
if none of the transaction's read or written keys changed after its snapshot.
Independent transactions touching different keys can both commit; dropping a
transaction aborts it without WAL I/O.

### Talk to it with curl

```sh
curl -u admin:secret -X PUT  localhost:6380/v1/keys/city -d Berlin   # -> OK
curl -u admin:secret         localhost:6380/v1/keys/city             # -> Berlin
curl -u admin:secret -X DELETE localhost:6380/v1/keys/city           # -> OK
curl                         localhost:6380/health                   # -> PONG
```

Because it's plain HTTP, any language or tool works (browser, Python `requests`,
JS `fetch`, etc.) — no special client required.

### Talk to it with the bundled client

Interactive REPL:

```sh
KVDB_USER=admin KVDB_PASSWORD=secret cargo run --bin kvdb-client
kvdb> SET greeting hello world
OK
kvdb> GET greeting
hello world
kvdb> DEL greeting
OK
kvdb> PING
PONG
```

One-shot (handy for scripts); credentials via env or `--user`/`--password`:

```sh
cargo run --bin kvdb-client -- http://127.0.0.1:6380 --user admin --password secret SET counter 42
cargo run --bin kvdb-client -- http://127.0.0.1:6380 --user admin --password secret GET counter
```

## Docker

```sh
docker build -t kvdb .
docker run -d \
  -e KVDB_USER=admin -e KVDB_PASSWORD=secret \
  -p 6380:6380 \
  -v kvdb-data:/data \
  kvdb

curl -u admin:secret -X PUT localhost:6380/v1/keys/city -d Berlin
```

Credentials are passed at run time and are never baked into the image. The WAL
lives on the `/data` volume, so data survives container restarts. The server
runs as a non-root user.

## Roadmap

The learning-oriented LSM foundation is complete: WAL recovery, versioned
memtables, indexed and Bloom-filtered SSTables, atomic batches, snapshots,
optimistic transactions, MVCC retention, compaction, HTTP access, load tests,
and a local performance baseline are implemented.

The next work is reliability-first: make acknowledged durability real, bound
recovery and memory use, propagate storage failures correctly, isolate blocking
I/O from the async server, and make delivery reproducible before adding more
database features. See the [prioritized roadmap](ROADMAP.md) for the audit
findings, milestones, and acceptance criteria.
