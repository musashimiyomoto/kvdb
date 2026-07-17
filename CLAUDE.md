# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`kvdb` is a learning-oriented networked key-value database in Rust (edition 2024). It implements the starting point of an LSM-tree engine: an in-memory memtable (`BTreeMap`) made durable by a write-ahead log, exposed over an HTTP/REST API with HTTP Basic auth. See the README's Roadmap for the intended trajectory (SSTables → compaction → bloom filters); the code is structured to grow into those stages.

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
cargo test --release --test load -- --ignored              # stress / concurrency correctness
cargo test --release --test perf -- --ignored --nocapture  # informational throughput (prints ops/sec)

# Build + smoke-test the Docker image the way CI does (see .github/workflows/ci.yml `docker` job)
docker build -t kvdb .

# Run the server (credentials are MANDATORY or it refuses to start)
KVDB_USER=admin KVDB_PASSWORD=secret cargo run --bin kvdb-server
# optional positional args: kvdb-server <BIND_ADDR> <WAL_PATH>  (defaults 0.0.0.0:6380, kvdb.wal)
# logging: KVDB_LOG=error|warn|info|debug (default info), KVDB_LOG_FILE=<path> (also append to file)
# flush tuning: KVDB_MEMTABLE_LIMIT=<n> live+tombstone entries before flush (default 1024)

# Run the client (REPL, or one-shot)
KVDB_USER=admin KVDB_PASSWORD=secret cargo run --bin kvdb-client
cargo run --bin kvdb-client -- http://127.0.0.1:6380 --user admin --password secret SET k v
```

## Architecture

Modules, layered bottom-up (`src/lib.rs` re-exports `Store`, `AppState`, `router`):

- **`src/log.rs` — logging.** A tiny dependency-free logger. `init()` (called once by the server) reads `KVDB_LOG` (level) and `KVDB_LOG_FILE` (optional file) from env and installs a global sink guarded by a `Mutex`; it always writes to stderr and additionally appends to the file when configured. Use the `log_error!/log_warn!/log_info!/log_debug!` macros (each takes an explicit `target` string first). Timestamps are UTC RFC3339, computed by hand (no `chrono`) in the same spirit as the hand-rolled base64/WAL code. Logging is best-effort: a failed write is never allowed to crash a request.

- **`src/store.rs` — storage engine.** `Store` is single-threaded and not internally synchronized; callers serialize access (the HTTP layer wraps it in a `Mutex`). The WAL is a custom binary format: `[op:u8][key_len:u32 BE][key][val_len:u32 BE][value]` for SET, `[op][key_len][key]` for DELETE. Durability ordering is the core invariant: **every mutation appends to the WAL and flushes to disk *before* the memtable is updated.** Recovery (`replay`) is forgiving by design — a truncated trailing record (crash mid-write) hits `UnexpectedEof` and is dropped as uncommitted rather than failing startup. An unknown op code, by contrast, is a hard `InvalidData` error. **Deletes are tombstones, not removals:** the memtable is `BTreeMap<Vec<u8>, Value>` where `Value` is `Set(bytes)` or `Tombstone`, so a delete can shadow an older on-disk value instead of resurrecting it. `get` maps a tombstone to `None`; `len`/`is_empty` count only live entries. `compact()` performs a full k-way merge of all SSTables, lets newer records win, drops tombstones because no older table remains, then publishes a replacement manifest before best-effort cleanup of superseded files.

- **`src/sstable.rs` — on-disk sorted tables + flush.** When the memtable exceeds `KVDB_MEMTABLE_LIMIT` entries (default 1024), it is flushed to an immutable, key-sorted SSTable file `<stem>-NNNNNN.sst`. Version 1 tables start with `KVDBSST1`, store the same Set/Tombstone records in logical 64-record blocks, then append a persisted sparse index, Bloom filter, and fixed footer. Each resident index entry contains the block's first key, byte range, and record count; the Bloom filter first rejects keys that definitely are not in the file, then possible hits binary-search the index and scan at most one block. Opening a table reads only the footer/index. Full-key operations such as `Store::len` explicitly scan table data rather than retaining a hidden dense index. Flush ordering remains **write+fsync the SSTable → atomically update the manifest (`<stem>.manifest`, temp+rename) → truncate the WAL → clear the memtable.** A crash between steps is safe: a half-written SSTable not yet in the manifest is an orphan and ignored, and un-truncated WAL data simply replays (the newer copy wins). Read path: memtable first, then SSTables newest-to-oldest, first hit wins **including a tombstone** (which yields `None`).

- **`src/http.rs` — REST layer.** `router(state)` builds the axum 0.8 router; the router is constructed here (not in the binary) specifically so integration tests can drive it via `tower`'s `oneshot` without binding a socket. `/health` is public; all `/v1/*` routes sit behind `auth` middleware (`route_layer`). `AppState` holds `Arc<Mutex<Store>>` plus credentials. Note: handlers return `lock_error()` (500) if the mutex is poisoned — a panic while holding the store lock degrades the whole server. Credential comparison uses `constant_time_eq` to avoid timing leaks.

- **`src/bin/{server,client}.rs` — thin binaries.** Server reads `KVDB_USER`/`KVDB_PASSWORD` from env (empty = unset = refuse to start), opens the store, serves the router. Client uses `reqwest` (built with `default-features = false` — **no TLS**, plain HTTP only; keeps the build/image lean).

## Tests

Integration tests live in `tests/` (not unit tests in `src`). `tests/http.rs` exercises the real router through `oneshot` and includes hand-rolled base64 for the auth header. Tests create isolated WAL files via per-test `tmp_path(tag)` helpers, or a whole isolated directory via `tmp_dir(tag)` when a test flushes (SSTables + manifest are siblings of the WAL) — when adding tests, give each a unique tag so files don't collide.

- `tests/store.rs`, `tests/http.rs` — the fast correctness suite (runs on `cargo test`).
- `tests/load.rs` — stress / concurrency correctness (bulk insert + many flushes, concurrent HTTP writes through a shared `Arc<Mutex<Store>>`). Every test is `#[ignore]`d; run with `--test load -- --ignored`.
- `tests/perf.rs` — a zero-dependency `Instant`-based micro-benchmark harness. Not pass/fail on timing (that would be flaky); it prints ops/sec and ns/op for set/get(memtable vs sstable vs miss)/flush/recovery. `#[ignore]`d; run with `--release --test perf -- --ignored --nocapture`.
- Some `#[cfg(test)]` unit tests do live in `src` (`log.rs`, `sstable.rs`) for module-internal helpers that aren't reachable from integration tests.

CI (`.github/workflows/ci.yml`) has two jobs: `ci` (fmt-check, clippy, release build, `cargo test`) and `docker` (builds the image, runs the container, and smoke-tests `/health` + a PUT/GET/DELETE roundtrip + persistence across a restart). The heavy `load`/`perf` suites are not run in CI by default.

## Conventions

- Keys and values are arbitrary bytes end-to-end (`Vec<u8>`); the HTTP layer carries values as the raw request/response body.
- When extending the WAL format, preserve backward-compatible replay (old logs must still recover) and keep the "torn tail is dropped, bad data is an error" distinction in `read_record`.
- A delete is a tombstone, never a silent `remove` — once SSTables exist, a removal would let an older on-disk value resurface. Preserve this whenever you touch the memtable or flush path.
- On-disk state (WAL + SSTables + manifest) shares one directory and stem, derived from the WAL path. Keep flush crash-safe by always ordering it SSTable → manifest → WAL-truncate, so an interrupted flush degrades to a replayable/ignorable state rather than data loss.
