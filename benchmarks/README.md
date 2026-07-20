# Controlled benchmark results

Files in this directory are raw stdout/stderr captures from the end-to-end
benchmark harness. They are evidence for roadmap rebaselines, not performance
gates or cross-machine comparisons.

Run the standard profile on an otherwise idle, persistent local filesystem:

```sh
cargo bench --locked --bench kvdb_bench -- \
  --profile standard --dir /path/on/the/device
```

Each capture includes the Git revision, Rust version, kernel, CPU, logical CPU
count, filesystem, workload size, throughput and latency distributions, plus
worker, cache, and compaction attribution counters. Compare medians and the
complete p50/p95/p99/max distribution; do not select the best sample.
