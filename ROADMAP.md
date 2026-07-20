# kvdb roadmap

Last reviewed: 2026-07-20.

kvdb has a working single-node LSM-style engine, baseline persistence
hardening, and a reproducible end-to-end benchmark. The current goal is to
finish the controlled standard-profile rebaseline for the implemented worker,
cache, and background-compaction changes, and only then resume file-format
hardening and API expansion.

## Verified baseline

The repository pins Rust 1.96.0 for local, CI, and Docker builds. The following
checks pass on that toolchain:

- `cargo fmt --all -- --check`
- `cargo clippy --all-targets --locked -- -D warnings`
- `cargo test --all --locked` (66 passing tests, 19 intentionally ignored)

The four ignored load tests, 15 component microbenchmarks, and the quick
end-to-end benchmark profile run as separate release-mode CI jobs. The
end-to-end harness uses an on-disk directory, explicit durability modes,
latency percentiles, randomized reads, overlapping compaction, and real TCP
concurrency. Shared-runner timing remains informational rather than a gate.

## Benchmark evidence

### Before storage worker/group commit

The 2026-07-20 quick profile ran three independent samples on the repository's
ext4-backed WSL volume with 128-byte values. These numbers guide priorities;
they are not an SLA or a cross-machine comparison.

| Scenario | Median throughput | Relevant latency |
|---|---:|---:|
| Buffered library SET | ~77k records/s | p99 ~89 us |
| Durable library SET | ~9 records/s | p50 ~77 ms; p99 ~791 ms |
| Durable batch of 100 | ~1.37k records/s | p50 ~67 ms per batch commit |
| Random warm SSTable GET | ~32k reads/s | p99 ~155 us |
| TCP GET, concurrency 8 | ~14k requests/s | p99 ~6.3 ms |
| Buffered TCP PUT, concurrency 8 | ~3.3k requests/s | p99 ~18.7 ms |
| Durable TCP PUT, concurrency 8 | ~11 requests/s | p99 ~1.73 s |
| Overlapping compaction, 50k input versions | ~64k versions/s | p50 ~783 ms total |

### After storage worker/group commit (quick profile)

These are iteration results from the same ext4-backed WSL environment, not the
controlled `standard` rebaseline required by Milestone 0.4.

| Scenario | Before | After | Relevant after latency |
|---|---:|---:|---:|
| Durable TCP PUT, concurrency 8 | ~11 req/s | ~85 req/s | p99 ~81 ms |
| Buffered TCP PUT, concurrency 8 | ~3.3k req/s | ~7.9k req/s | p99 ~5.6 ms |
| TCP GET, concurrency 8 | ~14k req/s | ~10.2k req/s | queue handoff regressed throughput |
| TCP GET, concurrency 32 | ~13.8k req/s | ~16.1k req/s | high-concurrency throughput improved |

For 20 concurrent durable writes, the benchmark observed three physical commit
groups with a maximum group size of eight. Durable concurrency-one remains
about 5 req/s, as expected: group commit removes redundant fsyncs only when
writes overlap. The worker adds a queue/thread handoff to reads, so the
low-concurrency GET regression is explicit optimization work, not hidden by the
write gains.

The first cache-enabled quick run compared both modes inside the same process
and workload. It is still a warm OS page-cache test; only kvdb's application
cache changes between rows.

| SSTable point-read mode | Median throughput | p99 | Cache evidence |
|---|---:|---:|---|
| Application cache disabled | ~20.3k reads/s | ~247 us | 21k block/file misses per sample |
| 64 MiB decoded-block cache | ~374k reads/s | ~12 us | 20,687 hits / 313 misses; 4.48 MiB resident |

That is about `18x` throughput and `20x` p99 improvement in this quick run.
The controlled standard-profile result remains part of Milestone 0.4.

The first background-compaction quick run (2026-07-20, same ext4-backed WSL
volume, three samples) merged five overlapping tables / 50k input versions with
a median measured merge duration of about `611 ms`. End-to-end start through
manifest publication was about `1.14 s` median. Concurrent foreground GETs ran
at about `297k reads/s` median with p99 about `61 us`; the reported peak merge
buffer was `1,696 bytes` and no twice-threshold foreground stall occurred. This
validates the harness and attribution counters, not the controlled comparison
required by Milestone 0.4.

The measurements identify three immediate bottlenecks:

- Per-mutation fsync dominates durable writes; batching 100 records improves
  throughput by roughly two orders of magnitude.
- Blocking storage behind the request-path mutex converts fsync time into
  queueing and second-scale TCP tail latency.
- Positive SSTable reads reopen files, while full compaction is inline and
  materializes every table; both paths add avoidable foreground work.

## Current execution order

This list is the implementation queue. Risk severity below does not override
it.

1. [x] Add a bounded storage worker and group commit.
2. [x] Add bounded SSTable file-handle and block caches.
3. [x] Move bounded streaming compaction out of the request path.
4. Run the standard benchmark on a controlled on-disk environment, finish
   detailed worker/cache instrumentation, and record
   the before/after baseline.
5. Add versioned checksummed WAL/SSTable/manifest formats and crash failpoints.
6. Continue service lifecycle, API contract, delivery, and operations work.

## Completed hardening

- **R1 - acknowledged durability:** default `durable` mode flushes and calls
  `sync_data`; explicitly weaker `buffered` mode is opt-in.
- **R3 - fallible reads:** SSTable failures propagate through `Store` and HTTP
  rather than becoming false `404 Not Found` responses.
- **R4 - bounded mutable state:** key, byte, version, and WAL-byte limits can
  independently trigger a flush; transaction conflict tracking is bounded.
- **R5 - bounded codecs:** public writes and WAL/SSTable decoders enforce key,
  value, batch, version, record, index, and Bloom limits before allocation.
- **R6 - single writer:** an advisory lifetime lock rejects a second writable
  `Store` for the same WAL.
- **R12 - reproducible build:** local, CI, and Docker builds use Rust 1.96.0
  and the repository lockfile with `--locked`.

Uncertain WAL, flush, or compaction errors poison the `Store`; publishing an
SSTable or manifest fsyncs its parent directory on Unix.

## Remaining risk register

`Severity` describes potential impact, not implementation order.

| ID | Severity | Status | Remaining risk |
|---|---|---|---|
| R2 | P0 | Partial | WAL records are not versioned, length-delimited frames and have no checksum or complete crash-point coverage. |
| R7 | P0 | Partial | WAL records, SSTable blocks, and manifest metadata lack checksums and a documented migration path. |
| R8 | P1 | Partial | A bounded worker and group commit replace the request-path mutex; queue/fsync timing, cancellation, and graceful worker shutdown remain. |
| R9 | P1 | Open | Basic credentials travel over plain HTTP; the client recognizes `https://` although reqwest has no TLS. |
| R10 | P1 | Open | One-shot client commands print HTTP errors but exit successfully, and values are decoded as text. |
| R11 | P1 | Open | REST exposes UTF-8 path keys and an implicit body limit while the library accepts binary keys and larger values. |
| R12 | P1 | Partial | Actions use mutable tags; dependency policy, scanning, SBOM, and image metadata remain open. |
| R13 | P1 | Implemented | Automatic compaction uses a streaming k-way merge on a background thread, manifest-atomic prefix replacement, suffix-safe concurrent flushes, twice-threshold backpressure, and run metrics; controlled rebaseline remains. |
| R14 | P2 | Open | There is no readiness probe, graceful shutdown, metrics, backup, verify, or repair command. |
| R15 | P1 | Implemented | Positive lookups use bounded file/decode-block LRU caches with metrics and compaction invalidation; controlled standard rebaseline remains. |

## Milestone 0: benchmark-driven performance (active)

### 0.1 Bounded storage worker and group commit

- [x] Give one dedicated blocking worker ownership of `Store`; communicate
  through a bounded request queue with explicit overload responses.
- [x] Group concurrent durable commits with configurable maximum group size and
  delay. Never acknowledge a member before the complete group is stable.
- [x] Preserve a unique sequence per logical commit and atomic semantics for
  each existing `WriteBatch` while sharing one WAL flush and fsync.
- [ ] Add request cancellation and graceful worker drain/join.
- Measure queue wait, group size, WAL write, fsync, and end-to-end commit
  latency. Group counts/sizes and saturation are covered; detailed timing,
  partial failure, cancellation, and shutdown coverage remain.

Acceptance: slow storage does not block unrelated Tokio work; queue memory is
bounded; concurrent commits share fsync without weakening durability; and the
standard benchmark shows the throughput/tail change against the current
baseline.

### 0.2 SSTable file and block caches

- [x] Reuse open SSTable files through a bounded file-handle LRU cache.
- [x] Add a bounded decoded-block LRU cache keyed by table path and byte range,
  with invalidation when compaction retires tables.
- [x] Export cache hit, miss, eviction, open-file, block, and resident-byte
  measurements.
- [x] Keep an explicit uncached benchmark path so correctness and cache benefit
  remain independently measurable.

Acceptance: repeated warm point reads avoid `File::open` and block decoding;
charged block memory stays within its configured limit. The quick profile shows
the expected throughput and p95/p99 improvement; standard-profile confirmation
remains in 0.4.

### 0.3 Background streaming compaction

- [x] Replace all-memory full compaction with a streaming k-way merge and bounded
  buffers.
- [x] Schedule compaction outside the foreground request operation with explicit
  write backpressure and crash-safe publication.
- [x] Report compaction input/output bytes, versions, duration, peak buffers, and
  foreground stalls.
- [x] Preserve MVCC, retention anchors, tombstones, and snapshot correctness across
  concurrent writes and process restart.

Acceptance: compaction memory is bounded independently of database size; no
request performs the full merge inline; overlapping-compaction tests preserve
the reference model; and foreground latency is reported during compaction. The
merge now buffers one record per input table (plus one merged key); the existing
sparse output index and Bloom filter remain proportional to output key count.
The end-to-end harness reports foreground GET latency and compaction bytes,
versions, duration, peak merge buffers, and stalls. Controlled standard-profile
numbers remain in 0.4.

### 0.4 Rebaseline

- Run `standard` on a documented on-disk environment before and after each
  optimization, retaining raw output and machine/filesystem metadata.
- Compare median throughput and p50/p95/p99/max rather than selecting the best
  run. Do not gate shared CI on timing.
- Record group size/fsync count and cache hit rate beside operation throughput
  so improvements can be attributed to the intended mechanism.

Acceptance: the repository contains one reproducible controlled-runner baseline
that demonstrates the effect of worker/group commit, caching, and background
compaction separately.

## Milestone 1: integrity and crash hardening

- Introduce a versioned, length-delimited WAL frame with a checksum and an
  explicit migration path from the current format.
- Add per-block SSTable checksums and checksummed manifest metadata.
- Bound manifest line/count parsing and validate filenames, duplicate entries,
  table ordering, sequence bounds, and table metadata during open.
- Add failpoints around WAL write/sync, SSTable sync/rename, manifest
  sync/rename, WAL truncation, and obsolete-table deletion. Kill a child at
  each point and compare recovery with acknowledged operations.
- Replace broad `io::Error` reporting with structured errors for invalid input,
  corruption, unavailable I/O, conflict, and poisoned state.
- Document supported filesystem/rename assumptions and safe cleanup of
  temporary and orphan files.

Acceptance: every acknowledged durable write survives the crash matrix;
corrupted or truncated data is rejected without panic or excessive allocation;
and uncertain writes prevent later writes until reopen/recovery.

## Milestone 2: service boundary and delivery

### 2.1 Lifecycle and limits

- Split `/live` from `/ready`; readiness fails while recovery is incomplete,
  storage is poisoned, or the queue remains saturated.
- Add connection, body, key/value, concurrency, and request timeout limits plus
  graceful SIGTERM/SIGINT draining.

### 2.2 HTTP and client contracts

- Choose a binary-key representation or explicitly define REST as UTF-8-only;
  preserve raw value bytes and publish exact limits.
- Add versioned machine-readable errors with stable status mappings.
- Expose atomic batches and compare-and-set before remote transactions.
- Return non-zero from one-shot client commands for authentication, transport,
  server, and unexpected not-found errors; support raw stdin/file/stdout.
- Add rustls HTTPS or formally require a TLS reverse proxy.

### 2.3 Secure delivery

- Pin Actions by commit SHA; add dependency/license policy, vulnerability
  scanning, scheduled updates, OCI labels, SBOM, and `HEALTHCHECK`.
- Document read-only-root-filesystem operation and test the exact non-root
  release image with persistent data.

Acceptance: lifecycle and API behavior have real TCP tests, builds remain
locked and reproducible, and dependency/image scans have no untriaged
high-severity findings.

## Milestone 3: predictable storage at scale

- Move from an SSTable-count trigger to size-aware tiered or leveled compaction
  with explicit write- and space-amplification targets.
- Tune cache and compaction using Bloom false-positive rate, read amplification,
  cache hit rate, and latency distributions from a controlled runner.
- Make snapshots and transactions lazy or structurally shared.
- Add prefix/range iteration required by backup and future APIs.

Acceptance: peak memory is bounded, foreground latency stays within a
documented budget, and model tests match a reference MVCC map across
compaction/restart cycles.

## Milestone 4: operations and observability

- Add structured tracing with request IDs and redaction. Export request,
  queue, WAL/fsync, memtable, SSTable, compaction, recovery, and corruption
  metrics.
- Add `kvdb verify`, consistent backup/export, restore/import, and explicit
  offline repair/salvage tooling; repair must never silently discard data.
- Add typed configuration with CLI/environment/default precedence and redacted
  effective-configuration logging.
- Document capacity planning, migration, backup recovery, disk-full,
  corruption, and graceful-shutdown runbooks.

## Cross-cutting test strategy

- Add property/model tests for batches, MVCC, retention, snapshots,
  transactions, and arbitrary reopen/flush/compact sequences.
- Fuzz every WAL/SSTable/manifest decoder under allocation limits; run Miri on
  focused codec/state-machine tests where practical.
- Add real TCP, multi-process lock, slow/disk-full I/O, and supported-platform
  coverage.
- Keep benchmarks explicit about warmup, dataset shape, durability mode,
  filesystem, and latency distributions.

## Feature work after hardening

After Milestones 0-3, prioritize range/prefix scans, conditional writes,
network batch APIs, online backup, and optional TTL with explicit MVCC
semantics.

Replication, clustering, distributed transactions, and automatic sharding stay
out of scope until single-node durability, bounded recovery, backpressure, and
operational recovery are demonstrated.
