# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`kvdb` is a learning-oriented networked key-value database in Rust (edition 2024). It implements an LSM-tree core: a versioned in-memory memtable (`BTreeMap`) made durable by a write-ahead log, immutable SSTables with sparse indexes and Bloom filters, full compaction, MVCC snapshots, and an HTTP/REST API with HTTP Basic auth.

## Commands

```sh
cargo build --release
cargo test                          # fast suite (lib + store + http); ignores load/perf
cargo test --test store             # only the storage-engine integration tests
cargo test --test http              # only the HTTP router tests
cargo test set_get_delete           # a single test by name
cargo clippy --all-targets          # lint
cargo fmt                           # format

# Heavy suites are #[ignore]d so the default run stays fast — opt in explicitly:
cargo test --release --test load -- --ignored --nocapture --test-threads=1
cargo test --release --test perf -- --ignored --nocapture --test-threads=1

# Build + smoke-test the Docker image the way CI does (see .github/workflows/ci.yml `docker` job)
docker build -t kvdb .

# Run the server (credentials are MANDATORY or it refuses to start)
KVDB_USER=admin KVDB_PASSWORD=secret cargo run --bin kvdb-server
# optional positional args: kvdb-server <BIND_ADDR> <WAL_PATH>  (defaults 0.0.0.0:6380, kvdb.wal)
# logging: KVDB_LOG=error|warn|info|debug (default info), KVDB_LOG_FILE=<path> (also append to file)
# storage tuning: KVDB_MEMTABLE_LIMIT=<n> entries before flush (default 1024)
#                 KVDB_COMPACTION_THRESHOLD=<n> SSTables before compaction (default 8; 0 disables)

# Run the client (REPL, or one-shot)
KVDB_USER=admin KVDB_PASSWORD=secret cargo run --bin kvdb-client
cargo run --bin kvdb-client -- http://127.0.0.1:6380 --user admin --password secret SET k v
```

## Architecture

Modules, layered bottom-up (`src/lib.rs` re-exports `Store`, `AppState`, `router`):

- **`src/log.rs` — logging.** A tiny dependency-free logger. `init()` (called once by the server) reads `KVDB_LOG` (level) and `KVDB_LOG_FILE` (optional file) from env and installs a global sink guarded by a `Mutex`; it always writes to stderr and additionally appends to the file when configured. Use the `log_error!/log_warn!/log_info!/log_debug!` macros (each takes an explicit `target` string first). Timestamps are UTC RFC3339, computed by hand (no `chrono`) in the same spirit as the hand-rolled base64/WAL code. Logging is best-effort: a failed write is never allowed to crash a request.

- **`src/store.rs` — storage engine.** `Store` is single-threaded and not internally synchronized; callers serialize access (the HTTP layer wraps it in a `Mutex`). Every standalone mutation and atomic `WriteBatch` gets one monotonically increasing `u64` commit sequence. WAL records encode `[op:u8][sequence:u64 BE]` before their payload; a batch additionally stores an operation count and its ordered SET/DELETE payloads inside one record. Durability ordering remains **append complete WAL record + flush before mutating the memtable**. Recovery decodes a complete record before applying it, so a torn trailing batch is discarded as a whole; unknown op codes, zero/non-increasing sequences, and malformed records are hard `InvalidData` errors. The manifest stores the latest SSTable sequence and a durable history boundary, while newer WAL-only sequences replay on top. The memtable is `BTreeMap<Vec<u8>, Vec<VersionedValue>>`, with versions ordered by commit sequence; `get_at` selects the newest version not newer than the requested sequence and rejects reads before the retained-history boundary. `snapshot()` copies the current logical state, while `snapshot_at(sequence)` reconstructs a historical state from retained versions. `begin_transaction()` adds a read-your-writes overlay and read set; `commit_transaction()` validates every read/write key against its last-modified sequence, then persists the overlay as one `WriteBatch`. Independent keys can commit concurrently, while a changed read key causes `TransactionError::Conflict`. **Deletes remain versioned tombstones**, so historical reads before and after a deletion are both correct. `compact()` performs a full k-way merge without further history loss; `compact_with_retention(sequence)` first flushes the memtable, then retains each key's boundary anchor plus newer versions, persists that boundary, and may discard obsolete tombstones. After each flush, the Store automatically compacts when the live SSTable count reaches `KVDB_COMPACTION_THRESHOLD` (default 8; zero disables automation).

- **`src/sstable.rs` — on-disk sorted tables + flush.** When the memtable exceeds `KVDB_MEMTABLE_LIMIT` entries (default 1024), it is flushed to an immutable, key-sorted SSTable file `<stem>-NNNNNN.sst`. Version 1 tables start with `KVDBSST1`; each key record stores a strictly increasing list of `[sequence, Set/Tombstone]` versions. Records are grouped into logical 64-key blocks, followed by a persisted sparse index, Bloom filter, and fixed footer. Each resident index entry contains the block's first key, byte range, and record count; the Bloom filter first rejects keys that definitely are not in the file, then possible hits binary-search the index and scan at most one block. `get_at` selects a version inside the matching record. Opening a table reads only the footer/index. Full-key operations such as `Store::len` explicitly scan table data rather than retaining a hidden dense index. Flush ordering remains **write+fsync the SSTable → atomically update the manifest (`<stem>.manifest`, temp+rename) → truncate the WAL → clear the memtable.** A crash between steps is safe: a half-written SSTable not yet in the manifest is an orphan and ignored, and un-truncated WAL data simply replays.

- **`src/http.rs` — REST layer.** `router(state)` builds the axum 0.8 router; the router is constructed here (not in the binary) specifically so integration tests can drive it via `tower`'s `oneshot` without binding a socket. `/health` is public; all `/v1/*` routes sit behind `auth` middleware (`route_layer`). `AppState` holds `Arc<Mutex<Store>>` plus credentials. Note: handlers return `lock_error()` (500) if the mutex is poisoned — a panic while holding the store lock degrades the whole server. Credential comparison uses `constant_time_eq` to avoid timing leaks.

- **`src/bin/{server,client}.rs` — thin binaries.** Server reads `KVDB_USER`/`KVDB_PASSWORD` from env (empty = unset = refuse to start), opens the store, serves the router. Client uses `reqwest` (built with `default-features = false` — **no TLS**, plain HTTP only; keeps the build/image lean).

## Tests

Integration tests live in `tests/` (not unit tests in `src`). `tests/http.rs` exercises the real router through `oneshot` and includes hand-rolled base64 for the auth header. Tests create isolated WAL files via per-test `tmp_path(tag)` helpers, or a whole isolated directory via `tmp_dir(tag)` when a test flushes (SSTables + manifest are siblings of the WAL) — when adding tests, give each a unique tag so files don't collide.

- `tests/store.rs`, `tests/http.rs` — the fast correctness suite (runs on `cargo test`).
- `tests/load.rs` — stress correctness: bulk persistence, concurrent HTTP writes, deterministic mixed operations checked against a reference map, and volume MVCC/retention checks. Every test is `#[ignore]`d; run sequentially with the command above.
- `tests/perf.rs` — a zero-dependency `Instant`-based benchmark harness covering WAL writes/batches/deletes, memtable/SSTable/historical reads, Bloom misses, flush, compaction, retention, recovery, snapshots, disk footprint, and the HTTP router. Timing is informational rather than pass/fail; run it sequentially to avoid benchmark interference.
- Some `#[cfg(test)]` unit tests do live in `src` (`log.rs`, `sstable.rs`) for module-internal helpers that aren't reachable from integration tests.

CI (`.github/workflows/ci.yml`) has four jobs: `ci` (fmt-check, clippy, release build, fast tests), `load` (sequential ignored release load tests), `performance` (sequential informational benchmarks with summary + artifact, no timing thresholds), and `docker` (image build plus `/health`, CRUD, and restart-persistence smoke tests).

## Conventions

- Keys and values are arbitrary bytes end-to-end (`Vec<u8>`); the HTTP layer carries values as the raw request/response body.
- This learning project uses one active on-disk format; treat a format change as an explicit coordinated migration. Keep the "torn tail is dropped, bad data is an error" distinction in `read_record`.
- A delete is a tombstone, never a silent `remove` — once SSTables exist, a removal would let an older on-disk value resurface. Preserve this whenever you touch the memtable or flush path.
- On-disk state (WAL + SSTables + manifest) shares one directory and stem, derived from the WAL path. Keep flush crash-safe by always ordering it SSTable → manifest → WAL-truncate, so an interrupted flush degrades to a replayable/ignorable state rather than data loss.
