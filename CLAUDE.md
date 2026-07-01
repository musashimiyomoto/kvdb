# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`kvdb` is a learning-oriented networked key-value database in Rust (edition 2024). It implements the starting point of an LSM-tree engine: an in-memory memtable (`BTreeMap`) made durable by a write-ahead log, exposed over an HTTP/REST API with HTTP Basic auth. See the README's Roadmap for the intended trajectory (SSTables → compaction → bloom filters); the code is structured to grow into those stages.

## Commands

```sh
cargo build --release
cargo test                          # all tests (lib + integration)
cargo test --test store             # only the storage-engine integration tests
cargo test --test http              # only the HTTP router tests
cargo test set_get_delete           # a single test by name
cargo clippy --all-targets          # lint
cargo fmt                           # format

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

- **`src/store.rs` — storage engine.** `Store` is single-threaded and not internally synchronized; callers serialize access (the HTTP layer wraps it in a `Mutex`). The WAL is a custom binary format: `[op:u8][key_len:u32 BE][key][val_len:u32 BE][value]` for SET, `[op][key_len][key]` for DELETE. Durability ordering is the core invariant: **every mutation appends to the WAL and flushes to disk *before* the memtable is updated.** Recovery (`replay`) is forgiving by design — a truncated trailing record (crash mid-write) hits `UnexpectedEof` and is dropped as uncommitted rather than failing startup. An unknown op code, by contrast, is a hard `InvalidData` error. **Deletes are tombstones, not removals:** the memtable is `BTreeMap<Vec<u8>, Value>` where `Value` is `Set(bytes)` or `Tombstone`, so a delete can shadow an older on-disk value instead of resurrecting it. `get` maps a tombstone to `None`; `len`/`is_empty` count only live entries.

- **`src/sstable.rs` — on-disk sorted tables + flush.** When the memtable exceeds `KVDB_MEMTABLE_LIMIT` entries (default 1024), it is flushed to an immutable, key-sorted SSTable file `<stem>-NNNNNN.sst` (record: `[flag:u8][key_len:u32 BE][key]` then `[val_len:u32 BE][value]` for a Set, nothing more for a Tombstone). Flush ordering is the durability invariant here: **write+fsync the SSTable → atomically update the manifest (`<stem>.manifest`, temp+rename) → truncate the WAL → clear the memtable.** A crash between steps is safe: a half-written SSTable not yet in the manifest is an orphan and ignored, and un-truncated WAL data simply replays (the newer copy wins). On `open`, the manifest lists live SSTables; each loads a **dense in-memory key→offset index** (a scan on open — sparse/block indexing is the next roadmap step). Read path: memtable first, then SSTables newest-to-oldest, first hit wins **including a tombstone** (which yields `None`).

- **`src/http.rs` — REST layer.** `router(state)` builds the axum 0.8 router; the router is constructed here (not in the binary) specifically so integration tests can drive it via `tower`'s `oneshot` without binding a socket. `/health` is public; all `/v1/*` routes sit behind `auth` middleware (`route_layer`). `AppState` holds `Arc<Mutex<Store>>` plus credentials. Note: handlers return `lock_error()` (500) if the mutex is poisoned — a panic while holding the store lock degrades the whole server. Credential comparison uses `constant_time_eq` to avoid timing leaks.

- **`src/bin/{server,client}.rs` — thin binaries.** Server reads `KVDB_USER`/`KVDB_PASSWORD` from env (empty = unset = refuse to start), opens the store, serves the router. Client uses `reqwest` (built with `default-features = false` — **no TLS**, plain HTTP only; keeps the build/image lean).

## Tests

Integration tests live in `tests/` (not unit tests in `src`). `tests/http.rs` exercises the real router through `oneshot` and includes hand-rolled base64 for the auth header. Tests create isolated WAL files via per-test `tmp_path(tag)` helpers — when adding tests, give each a unique tag so WAL files don't collide.

## Conventions

- Keys and values are arbitrary bytes end-to-end (`Vec<u8>`); the HTTP layer carries values as the raw request/response body.
- When extending the WAL format, preserve backward-compatible replay (old logs must still recover) and keep the "torn tail is dropped, bad data is an error" distinction in `read_record`.
- A delete is a tombstone, never a silent `remove` — once SSTables exist, a removal would let an older on-disk value resurface. Preserve this whenever you touch the memtable or flush path.
- On-disk state (WAL + SSTables + manifest) shares one directory and stem, derived from the WAL path. Keep flush crash-safe by always ordering it SSTable → manifest → WAL-truncate, so an interrupted flush degrades to a replayable/ignorable state rather than data loss.
