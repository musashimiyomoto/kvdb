# kvdb roadmap

Last reviewed: 2026-07-20.

kvdb now has a working single-node LSM-style storage engine and its first
persistence-hardening pass. The immediate goal is to finish corruption and
crash handling before expanding the network API or adding new database
features.

## Verified baseline

The repository pins Rust 1.96.0 for local, CI, and Docker builds. The following
checks pass on that toolchain:

- `cargo fmt --all -- --check`
- `cargo clippy --all-targets --locked -- -D warnings`
- `cargo test --all --locked` (60 passing tests, 19 intentionally ignored)

The four ignored load tests, 15 component microbenchmarks, and the quick
end-to-end benchmark profile remain separate release-mode jobs in CI. The
end-to-end harness uses an on-disk directory, explicit durability modes,
latency percentiles, randomized reads, and real TCP concurrency. Docker copies
`Cargo.lock`, builds with `--locked`, runs as a non-root user, and has a
persistence smoke test in CI.

## Completed hardening

The following findings from the original audit are implemented. Storage
behaviors have regression coverage in `tests/store.rs` and `tests/http.rs`:

- **R1 - acknowledged durability:** the default `durable` mode flushes and
  calls `sync_data` before acknowledging each mutation. The explicitly weaker
  `buffered` mode is opt-in.
- **R3 - fallible reads:** SSTable read failures propagate through `Store` and
  become HTTP storage errors instead of false `404 Not Found` responses.
- **R4 - bounded mutable state:** distinct-key, approximate-byte, version-count,
  and WAL-byte limits can each trigger a memtable flush. Transaction conflict
  checks no longer retain an unbounded per-key sequence map.
- **R5 - bounded codecs:** public writes and WAL/SSTable decoders enforce key,
  value, batch, version, record, index, and Bloom metadata limits before large
  allocations.
- **R6 - single writer:** an advisory lifetime lock rejects a second writable
  `Store` for the same WAL.
- **R12 - reproducible Rust build:** `rust-toolchain.toml`, CI, and Docker use
  Rust 1.96.0; Docker consumes the repository lockfile with `--locked`.

Storage writes also fail closed: an uncertain WAL, flush, or compaction error
poisons that `Store` instance, and publishing an SSTable or manifest fsyncs its
parent directory on Unix. These changes reduce R2 and R7 but do not close them
because file formats still lack checksums and crash-point coverage.

## Remaining risk register

| ID | Priority | Status | Remaining risk |
|---|---|---|---|
| R2 | P0 | Partial | WAL records are not versioned, length-delimited frames and have no checksum; crash/failpoint tests do not yet exercise every write boundary. |
| R7 | P0 | Partial | WAL records, SSTable blocks, and manifest metadata have no integrity checksums or documented format-migration path. |
| R8 | P1 | Open | Axum handlers hold `std::sync::Mutex<Store>` during blocking filesystem I/O, so a slow disk can block request workers. |
| R9 | P1 | Open | Basic credentials travel over plain HTTP; the client recognizes `https://` URLs even though reqwest is built without TLS. |
| R10 | P1 | Open | One-shot client commands print HTTP errors but still exit successfully, and response values are always decoded as text. |
| R11 | P1 | Open | The REST API exposes UTF-8 path keys and an implicit Axum body limit, while the library accepts binary keys and values up to its own limits. |
| R12 | P1 | Partial | GitHub Actions still use mutable version tags; dependency policy, vulnerability scanning, SBOM generation, and image metadata remain open. |
| R13 | P2 | Open | Full compaction materializes all live SSTables in memory and runs inline with the triggering write. |
| R14 | P2 | Open | There is no readiness probe, graceful shutdown, metrics, backup, verify, or repair command. |

## Milestone 0: trustworthy persistence (P0, in progress)

### Done

- [x] Sync acknowledged writes by default and document buffered durability.
- [x] Poison a store after uncertain storage writes.
- [x] Fsync the parent directory after publishing SSTables and manifests on
  Unix.
- [x] Make current reads fallible and propagate storage errors through HTTP.
- [x] Bound public inputs and WAL/SSTable decoder allocations.
- [x] Bound memtable keys, bytes, versions, and WAL size.
- [x] Enforce one writable process per WAL with a lifetime lock.

### Next

- [ ] Introduce a versioned, length-delimited WAL frame with a checksum while
  retaining an explicit migration path from the current format.
- [ ] Add per-block SSTable checksums and checksummed manifest metadata.
- [ ] Bound manifest line/count parsing and validate filenames, duplicate
  entries, table order, sequence bounds, and table metadata during open.
- [ ] Add failpoints around WAL write/sync, SSTable sync/rename, manifest
  sync/rename, WAL truncation, and obsolete-table deletion. Kill a child
  process at each point and compare recovery with acknowledged operations.
- [ ] Replace broad `io::Error` reporting with structured errors for invalid
  input, corruption, unavailable I/O, conflicts, and poisoned state.
- [ ] Define the supported filesystem/rename assumptions and a safe policy for
  cleaning temporary and orphan files.

Acceptance: every acknowledged durable write survives the tested crash matrix;
corrupted or truncated data is rejected without panic or excessive allocation;
and an uncertain write prevents all later writes until reopen/recovery.

## Milestone 1: production-safe service boundary (P1)

### 1.1 Isolate blocking storage work

- Give one dedicated blocking worker ownership of `Store` and communicate
  through a bounded request queue with cancellation and overload responses.
- Add optional group commit so concurrent durable writes can share an fsync
  without weakening the selected durability mode.
- Split `/live` from `/ready`; readiness must fail while recovery is incomplete,
  storage is poisoned, or the queue remains saturated.
- Add connection, request-body, key/value, concurrency, and request timeout
  limits, plus graceful SIGTERM/SIGINT draining.

Acceptance: slow-disk tests do not block unrelated Tokio tasks, overload stays
bounded and observable, and shutdown either completes accepted work or clearly
rejects it.

### 1.2 Define the HTTP and client contracts

- Choose a binary-key representation such as base64url, or explicitly define
  the REST API as UTF-8-only. Preserve raw value bytes and publish exact limits.
- Add versioned machine-readable errors and stable mappings for not found,
  conflict, invalid input, too large, unavailable, and corruption.
- Expose atomic batches and compare-and-set before considering remote
  transactions.
- Make one-shot client commands return non-zero for authentication, transport,
  server, and unexpected not-found errors. Add strict argument validation,
  timeouts, raw stdin/file input, and raw stdout output.
- Add rustls HTTPS support or formally require a TLS reverse proxy and reject or
  document plaintext Basic Auth outside trusted local deployments.

Acceptance: TCP-level tests cover binary data, limits, status/exit-code mapping,
timeouts, the TLS policy, and server restart behavior.

### 1.3 Secure delivery

- Pin GitHub Actions by commit SHA and add dependency/license policy,
  vulnerability scanning, and scheduled dependency updates.
- Add OCI labels, a container `HEALTHCHECK`, read-only-root-filesystem guidance,
  an SBOM, and a non-root persistence test using the exact release image.
- Keep load tests correctness-only. Track statistical performance regressions
  on a controlled runner before introducing timing gates.

Acceptance: builds use the same locked dependency graph and Rust version, and
dependency/image scans have no untriaged high-severity findings.

## Milestone 2: predictable storage at scale (P2)

- Replace all-in-memory full compaction with a streaming k-way merge and bounded
  buffers. Run compaction outside the request path with write backpressure and
  crash-safe publication.
- Move from an SSTable-count trigger to size-aware tiered or leveled compaction
  with explicit write- and space-amplification targets.
- Add bounded block and file-handle caches. Measure Bloom false-positive rate,
  read amplification, cache hit rate, and p50/p95/p99 latency.
- Make snapshots and transactions lazy or structurally shared so beginning a
  transaction does not copy the whole visible database.
- Add prefix/range iteration primitives needed by backup and future APIs.

Acceptance: compaction peak memory is bounded independently of database size,
foreground latency remains within a documented budget, and model tests still
match a reference MVCC map across compaction and restart cycles.

## Milestone 3: operations and observability (P2)

- Adopt structured tracing with request IDs and redaction. Export metrics for
  request latency/status, queue depth, WAL/fsync latency, memtable bytes,
  SSTable count/bytes, compaction, recovery, and corruption.
- Add `kvdb verify`, consistent backup/export, restore/import, and explicit
  offline repair/salvage tooling. Automatic repair must never silently discard
  data.
- Add typed configuration with CLI/environment/default precedence, startup
  validation, and a redacted effective-configuration log.
- Document capacity planning, format migration, backup recovery, disk-full and
  corruption response, and graceful shutdown runbooks.

Acceptance: a fresh operator can detect an unhealthy store, create and restore
a verified backup, and follow tested disk-full and corruption runbooks.

## Cross-cutting test strategy

- Add property/model tests for batches, MVCC, retention, snapshots, transaction
  conflicts, and arbitrary reopen/flush/compact sequences.
- Fuzz every WAL/SSTable/manifest decoder under allocation limits; run Miri on
  focused codec and state-machine tests where practical.
- Add real TCP server/client tests, multi-process lock tests, slow/disk-full I/O
  tests, and platform coverage for every explicitly supported target.
- Keep benchmark reports explicit about warmup, dataset shape, durability mode,
  and latency distributions.

## Feature work after hardening (P3)

After Milestones 0-2, prioritize features that build on the proven single-node
engine: range/prefix scans, conditional writes, network batch APIs, online
backup, and optional TTL with explicitly defined MVCC semantics.

Replication, clustering, distributed transactions, and automatic sharding stay
out of scope until single-node durability, bounded recovery, backpressure, and
operational recovery are demonstrated.
