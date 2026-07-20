//! Reproducible end-to-end benchmark harness for kvdb.
//!
//! Unlike the component microbenchmarks in `tests/perf.rs`, this executable
//! uses an on-disk directory by default, selects durability explicitly, reports
//! latency percentiles, randomizes point reads, and exercises real TCP.

use std::env;
use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use kvdb::{AppState, Durability, Store, WriteBatch, router};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::sync::Barrier;

const VALUE_BYTES: usize = 128;
const BATCH_SIZE: usize = 100;
const LATENCY_SAMPLES: usize = 2_048;

#[derive(Clone, Copy)]
enum Profile {
    Quick,
    Standard,
}

impl Profile {
    fn parse(value: &str) -> Self {
        match value {
            "quick" => Self::Quick,
            "standard" => Self::Standard,
            other => panic!("unknown profile {other:?}; expected quick or standard"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Quick => "quick",
            Self::Standard => "standard",
        }
    }

    fn workload(self) -> Workload {
        match self {
            Self::Quick => Workload {
                samples: 3,
                durable_writes: 40,
                durable_batch_records: 500,
                durable_tcp_writes: 20,
                buffered_writes: 20_000,
                read_keys: 20_000,
                read_ops: 20_000,
                tcp_ops: 1_000,
                compaction_keys: 10_000,
                compaction_tables: 5,
            },
            Self::Standard => Workload {
                samples: 5,
                durable_writes: 500,
                durable_batch_records: 10_000,
                durable_tcp_writes: 200,
                buffered_writes: 200_000,
                read_keys: 100_000,
                read_ops: 100_000,
                tcp_ops: 10_000,
                compaction_keys: 50_000,
                compaction_tables: 6,
            },
        }
    }
}

struct Config {
    profile: Profile,
    base_dir: PathBuf,
    allow_memory_fs: bool,
    keep: bool,
}

impl Config {
    fn parse() -> Self {
        let mut profile = Profile::Standard;
        let mut base_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("kvdb-bench");
        let mut allow_memory_fs = false;
        let mut keep = false;
        let mut args = env::args().skip(1);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                // Cargo passes this marker to benchmark executables even when
                // the built-in libtest harness is disabled.
                "--bench" => {}
                "--profile" => {
                    profile = Profile::parse(&args.next().expect("--profile requires a value"));
                }
                "--dir" => {
                    base_dir = PathBuf::from(args.next().expect("--dir requires a path"));
                }
                "--allow-memory-fs" => allow_memory_fs = true,
                "--keep" => keep = true,
                "-h" | "--help" => {
                    println!(
                        "usage: cargo bench --bench kvdb_bench -- \
                         [--profile quick|standard] [--dir PATH] \
                         [--allow-memory-fs] [--keep]"
                    );
                    std::process::exit(0);
                }
                other => panic!("unknown argument {other:?}; use --help"),
            }
        }

        Self {
            profile,
            base_dir,
            allow_memory_fs,
            keep,
        }
    }
}

#[derive(Clone, Copy)]
struct Workload {
    samples: usize,
    durable_writes: usize,
    durable_batch_records: usize,
    durable_tcp_writes: usize,
    buffered_writes: usize,
    read_keys: usize,
    read_ops: usize,
    tcp_ops: usize,
    compaction_keys: usize,
    compaction_tables: usize,
}

struct RunDirectory {
    path: PathBuf,
    keep: bool,
}

impl RunDirectory {
    fn new(config: &Config) -> Self {
        fs::create_dir_all(&config.base_dir).expect("create benchmark base directory");
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos();
        let path = config
            .base_dir
            .join(format!("run-{}-{nonce}", std::process::id()));
        fs::create_dir(&path).expect("create unique benchmark run directory");
        Self {
            path,
            keep: config.keep,
        }
    }

    fn sample(&self, scenario: &str, sample: usize) -> PathBuf {
        let path = self.path.join(format!("{scenario}-{sample}"));
        fs::create_dir(&path).expect("create benchmark sample directory");
        path
    }

    fn clean_sample(&self, path: &Path) {
        if !self.keep {
            fs::remove_dir_all(path).expect("remove benchmark sample directory");
        }
    }
}

impl Drop for RunDirectory {
    fn drop(&mut self) {
        if self.keep {
            println!("BENCH_DATA kept={}", self.path.display());
        } else if let Err(error) = fs::remove_dir_all(&self.path) {
            eprintln!(
                "warning: could not remove benchmark directory {}: {error}",
                self.path.display()
            );
        }
    }
}

#[derive(Default)]
struct ResultSet {
    throughputs: Vec<f64>,
    latencies_ns: Vec<u64>,
}

impl ResultSet {
    fn push(&mut self, measurement: Measurement, units: usize) {
        self.throughputs
            .push(units as f64 / measurement.elapsed.as_secs_f64());
        self.latencies_ns.extend(measurement.latencies_ns);
    }

    fn report(&mut self, name: &str, throughput_unit: &str, latency_unit: &str) {
        self.throughputs.sort_by(f64::total_cmp);
        self.latencies_ns.sort_unstable();
        let median = percentile_f64(&self.throughputs, 0.50);
        let min = self.throughputs.first().copied().unwrap_or_default();
        let max = self.throughputs.last().copied().unwrap_or_default();
        let p50 = percentile_u64(&self.latencies_ns, 0.50) as f64 / 1_000.0;
        let p95 = percentile_u64(&self.latencies_ns, 0.95) as f64 / 1_000.0;
        let p99 = percentile_u64(&self.latencies_ns, 0.99) as f64 / 1_000.0;
        let max_latency = self.latencies_ns.last().copied().unwrap_or_default() as f64 / 1_000.0;

        println!(
            "RESULT name={name:?} throughput_unit={throughput_unit} \
             latency_unit={latency_unit} samples={} median_per_sec={median:.0} \
             min_per_sec={min:.0} max_per_sec={max:.0} p50_us={p50:.3} \
             p95_us={p95:.3} p99_us={p99:.3} max_us={max_latency:.3}",
            self.throughputs.len()
        );
    }
}

struct Measurement {
    elapsed: Duration,
    latencies_ns: Vec<u64>,
}

fn measure(operations: usize, mut operation: impl FnMut(usize)) -> Measurement {
    let stride = (operations / LATENCY_SAMPLES).max(1);
    let mut latencies_ns = Vec::with_capacity(operations.div_ceil(stride));
    let total_start = Instant::now();

    for operation_index in 0..operations {
        if operation_index % stride == 0 {
            let start = Instant::now();
            operation(operation_index);
            latencies_ns.push(duration_ns(start.elapsed()));
        } else {
            operation(operation_index);
        }
    }

    Measurement {
        elapsed: total_start.elapsed(),
        latencies_ns,
    }
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn percentile_u64(values: &[u64], quantile: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values[percentile_index(values.len(), quantile)]
}

fn percentile_f64(values: &[f64], quantile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values[percentile_index(values.len(), quantile)]
}

fn percentile_index(len: usize, quantile: f64) -> usize {
    (((len - 1) as f64 * quantile).round() as usize).min(len - 1)
}

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:012}").into_bytes()
}

fn value(index: usize) -> Vec<u8> {
    let mut value = vec![b'x'; VALUE_BYTES];
    let suffix = index.to_le_bytes();
    value[..suffix.len()].copy_from_slice(&suffix);
    value
}

fn keys(count: usize) -> Vec<Vec<u8>> {
    (0..count).map(key).collect()
}

fn configure_store(store: &mut Store, durability: Durability, keep_in_memtable: bool) {
    store.set_durability(durability);
    store.set_compaction_threshold(0);
    if keep_in_memtable {
        store.set_memtable_limit(usize::MAX);
        store.set_memtable_bytes_limit(usize::MAX);
        store.set_memtable_versions_limit(usize::MAX);
        store.set_wal_bytes_limit(u64::MAX);
    }
}

fn populate(store: &mut Store, all_keys: &[Vec<u8>], version: usize) {
    for chunk in all_keys.chunks(10_000) {
        let mut batch = WriteBatch::new();
        for (offset, key) in chunk.iter().enumerate() {
            batch.set(key.clone(), value(version.wrapping_add(offset)));
        }
        store.write_batch(batch).expect("populate store");
    }
}

fn bench_writes(run: &RunDirectory, workload: Workload) {
    for (name, durability, operations) in [
        (
            "set_buffered",
            Durability::Buffered,
            workload.buffered_writes,
        ),
        ("set_durable", Durability::Durable, workload.durable_writes),
    ] {
        let all_keys = keys(operations);
        let payload = value(0);
        let mut results = ResultSet::default();
        for sample in 0..workload.samples {
            let dir = run.sample(name, sample);
            let mut store = Store::open(dir.join("kvdb.wal")).expect("open write benchmark store");
            configure_store(&mut store, durability, true);
            let measurement = measure(operations, |index| {
                store
                    .set(all_keys[index].clone(), payload.clone())
                    .expect("benchmark SET");
            });
            drop(store);
            run.clean_sample(&dir);
            results.push(measurement, operations);
        }
        results.report(name, "records", "record");
    }

    let records = workload.durable_batch_records;
    let batches = records.div_ceil(BATCH_SIZE);
    let all_keys = keys(records);
    let payload = value(0);
    let mut results = ResultSet::default();
    for sample in 0..workload.samples {
        let dir = run.sample("batch_durable_100", sample);
        let mut store = Store::open(dir.join("kvdb.wal")).expect("open batch benchmark store");
        configure_store(&mut store, Durability::Durable, true);
        let measurement = measure(batches, |batch_index| {
            let start = batch_index * BATCH_SIZE;
            let end = (start + BATCH_SIZE).min(records);
            let mut batch = WriteBatch::new();
            for key in &all_keys[start..end] {
                batch.set(key.clone(), payload.clone());
            }
            store.write_batch(batch).expect("benchmark durable batch");
        });
        drop(store);
        run.clean_sample(&dir);
        results.push(measurement, records);
    }
    results.report("batch_durable_100", "records", "batch_commit");
}

fn bench_reads(run: &RunDirectory, workload: Workload) {
    let all_keys = keys(workload.read_keys);
    for (name, flush, cache_limits) in [
        ("get_memtable_random", false, None),
        ("get_sstable_warm_uncached_random", true, Some((0, 0))),
        (
            "get_sstable_warm_cached_random",
            true,
            Some((64, 64 * 1024 * 1024)),
        ),
    ] {
        let mut results = ResultSet::default();
        for sample in 0..workload.samples {
            let dir = run.sample(name, sample);
            let mut store = Store::open(dir.join("kvdb.wal")).expect("open read benchmark store");
            configure_store(&mut store, Durability::Buffered, true);
            if let Some((file_capacity, block_bytes)) = cache_limits {
                store.set_sstable_cache_limits(file_capacity, block_bytes);
            }
            populate(&mut store, &all_keys, 0);
            if flush {
                store.flush().expect("flush read benchmark store");
            }

            let mut random = XorShift64::new(0x4d59_5df4_d0f3_3173 ^ sample as u64);
            for _ in 0..1_000.min(workload.read_ops) {
                let index = random.next_usize(workload.read_keys);
                black_box(store.get(&all_keys[index]).expect("warm read"));
            }
            let measurement = measure(workload.read_ops, |_| {
                let index = random.next_usize(workload.read_keys);
                black_box(store.get(&all_keys[index]).expect("benchmark GET"));
            });
            if flush {
                let cache = store.sstable_cache_metrics();
                println!(
                    "SSTABLE_CACHE name={name:?} sample={sample} file_hits={} file_misses={} \
                     file_evictions={} open_files={} block_hits={} block_misses={} \
                     block_evictions={} resident_blocks={} resident_bytes={}",
                    cache.file_hits,
                    cache.file_misses,
                    cache.file_evictions,
                    cache.open_files,
                    cache.block_hits,
                    cache.block_misses,
                    cache.block_evictions,
                    cache.resident_blocks,
                    cache.resident_bytes
                );
            }
            drop(store);
            run.clean_sample(&dir);
            results.push(measurement, workload.read_ops);
        }
        results.report(name, "reads", "read");
    }
}

fn bench_recovery(run: &RunDirectory, workload: Workload) {
    let all_keys = keys(workload.buffered_writes);
    let payload = value(0);
    let mut results = ResultSet::default();
    for sample in 0..workload.samples {
        let dir = run.sample("wal_recovery_warm", sample);
        let wal = dir.join("kvdb.wal");
        {
            let mut store = Store::open(&wal).expect("open recovery setup store");
            configure_store(&mut store, Durability::Buffered, true);
            for key in &all_keys {
                store
                    .set(key.clone(), payload.clone())
                    .expect("write recovery setup WAL");
            }
        }
        let measurement = measure(1, |_| {
            let store = Store::open(&wal).expect("reopen recovery benchmark store");
            assert_eq!(store.len().expect("count recovered keys"), all_keys.len());
            black_box(store);
        });
        run.clean_sample(&dir);
        results.push(measurement, all_keys.len());
    }
    results.report("wal_recovery_warm", "records", "reopen");
}

fn bench_compaction(run: &RunDirectory, workload: Workload) {
    let all_keys = keys(workload.compaction_keys);
    let input_versions = workload.compaction_keys * workload.compaction_tables;
    let mut results = ResultSet::default();
    let mut foreground_reads = ResultSet::default();
    for sample in 0..workload.samples {
        let dir = run.sample("compaction_overlapping", sample);
        let mut store = Store::open(dir.join("kvdb.wal")).expect("open compaction store");
        configure_store(&mut store, Durability::Buffered, true);
        for version in 0..workload.compaction_tables {
            populate(&mut store, &all_keys, version * workload.compaction_keys);
            store.flush().expect("flush compaction input table");
        }
        let before = directory_bytes(&dir);
        let compaction_start = Instant::now();
        assert!(
            store
                .compact_in_background()
                .expect("start background compaction")
        );
        let foreground = measure(all_keys.len(), |index| {
            black_box(
                store
                    .get(&all_keys[index])
                    .expect("foreground GET during compaction"),
            );
        });
        store
            .wait_for_background_compaction()
            .expect("publish background compaction");
        let compaction_elapsed = compaction_start.elapsed();
        let measurement = Measurement {
            elapsed: compaction_elapsed,
            latencies_ns: vec![duration_ns(compaction_elapsed)],
        };
        let after = directory_bytes(&dir);
        let metrics = store.compaction_metrics();
        println!(
            "STORAGE name=\"compaction_overlapping\" sample={sample} before_bytes={before} \
             after_bytes={after} input_tables={} input_bytes={} output_bytes={} \
             input_versions={} output_versions={} duration_us={} peak_buffer_bytes={} \
             foreground_stalls={}",
            metrics.input_tables,
            metrics.input_bytes,
            metrics.output_bytes,
            metrics.input_versions,
            metrics.output_versions,
            metrics.duration_micros,
            metrics.peak_buffer_bytes,
            metrics.foreground_stalls
        );
        drop(store);
        run.clean_sample(&dir);
        results.push(measurement, input_versions);
        foreground_reads.push(foreground, all_keys.len());
    }
    results.report("compaction_overlapping", "input_versions", "compaction");
    foreground_reads.report(
        "get_during_background_compaction",
        "reads",
        "foreground_get",
    );
}

fn bench_tcp(run: &RunDirectory, workload: Workload) {
    let runtime = Runtime::new().expect("create Tokio benchmark runtime");
    for concurrency in [1, 8, 32] {
        let name = format!("tcp_get_c{concurrency}");
        let mut results = ResultSet::default();
        for sample in 0..workload.samples {
            let dir = run.sample(&name, sample);
            let measurement = runtime.block_on(tcp_sample(
                dir,
                workload.read_keys,
                workload.tcp_ops,
                concurrency,
                false,
                Durability::Buffered,
                run.keep,
            ));
            results.push(measurement, workload.tcp_ops);
        }
        results.report(&name, "requests", "request");
    }

    for concurrency in [1, 8] {
        let name = format!("tcp_put_buffered_c{concurrency}");
        let mut results = ResultSet::default();
        for sample in 0..workload.samples {
            let dir = run.sample(&name, sample);
            let measurement = runtime.block_on(tcp_sample(
                dir,
                workload.read_keys,
                workload.tcp_ops,
                concurrency,
                true,
                Durability::Buffered,
                run.keep,
            ));
            results.push(measurement, workload.tcp_ops);
        }
        results.report(&name, "requests", "request");
    }

    for concurrency in [1, 8] {
        let name = format!("tcp_put_durable_c{concurrency}");
        let mut results = ResultSet::default();
        for sample in 0..workload.samples {
            let dir = run.sample(&name, sample);
            let measurement = runtime.block_on(tcp_sample(
                dir,
                workload.read_keys,
                workload.durable_tcp_writes,
                concurrency,
                true,
                Durability::Durable,
                run.keep,
            ));
            results.push(measurement, workload.durable_tcp_writes);
        }
        results.report(&name, "requests", "request");
    }
}

async fn tcp_sample(
    dir: PathBuf,
    read_keys: usize,
    requests: usize,
    concurrency: usize,
    write: bool,
    durability: Durability,
    keep: bool,
) -> Measurement {
    let wal = dir.join("kvdb.wal");
    let mut store = Store::open(&wal).expect("open TCP benchmark store");
    configure_store(&mut store, durability, true);
    if !write {
        populate(&mut store, &keys(read_keys), 0);
    }

    let state = AppState::new(store, "admin", "secret");
    let app = router(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind TCP benchmark server");
    let address = listener.local_addr().expect("read TCP server address");
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve TCP benchmark");
    });

    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(concurrency)
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build benchmark HTTP client");
    warm_http_connections(&client, address, concurrency).await;
    let next = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let mut workers = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let client = client.clone();
        let next = Arc::clone(&next);
        let barrier = Arc::clone(&barrier);
        workers.push(tokio::spawn(async move {
            let mut latencies = Vec::new();
            barrier.wait().await;
            loop {
                let operation = next.fetch_add(1, Ordering::Relaxed);
                if operation >= requests {
                    break;
                }
                let key_index = if write {
                    operation
                } else {
                    mixed_index(operation, read_keys)
                };
                let url = format!("http://{address}/v1/keys/key-{key_index:012}");
                let start = Instant::now();
                let request = if write {
                    client.put(url).body(value(operation))
                } else {
                    client.get(url)
                };
                let response = request
                    .basic_auth("admin", Some("secret"))
                    .send()
                    .await
                    .expect("send benchmark HTTP request");
                assert_eq!(response.status(), reqwest::StatusCode::OK);
                black_box(response.bytes().await.expect("read benchmark response"));
                latencies.push(duration_ns(start.elapsed()));
            }
            latencies
        }));
    }

    let start = Instant::now();
    barrier.wait().await;
    let mut latencies_ns = Vec::with_capacity(requests);
    for worker in workers {
        latencies_ns.extend(worker.await.expect("join benchmark HTTP worker"));
    }
    let elapsed = start.elapsed();
    server.abort();
    let _ = server.await;

    let metrics = state.storage_metrics();
    let operation = if write { "PUT" } else { "GET" };
    println!(
        "STORAGE_WORKER operation={operation} durability={durability:?} \
         concurrency={concurrency} requests={requests} dequeued={} groups={} \
         max_group_size={} queue_full={} queue_wait_us={} max_queue_wait_us={} \
         group_commit_us={} max_group_commit_us={} wal_write_us={} wal_flush_us={} \
         wal_sync_us={}",
        metrics.dequeued_commands,
        metrics.write_groups,
        metrics.max_group_size,
        metrics.queue_full,
        metrics.queue_wait_micros,
        metrics.max_queue_wait_micros,
        metrics.group_commit_micros,
        metrics.max_group_commit_micros,
        metrics.wal_write_micros,
        metrics.wal_flush_micros,
        metrics.wal_sync_micros
    );

    if !keep {
        fs::remove_dir_all(&dir).expect("remove TCP benchmark sample directory");
    }

    Measurement {
        elapsed,
        latencies_ns,
    }
}

async fn warm_http_connections(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    concurrency: usize,
) {
    let mut warmups = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let client = client.clone();
        warmups.push(tokio::spawn(async move {
            let response = client
                .get(format!("http://{address}/health"))
                .send()
                .await
                .expect("warm benchmark HTTP connection");
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            black_box(response.bytes().await.expect("read warmup response"));
        }));
    }
    for warmup in warmups {
        warmup.await.expect("join HTTP connection warmup");
    }
}

fn mixed_index(index: usize, upper_bound: usize) -> usize {
    let mut value = (index as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    ((value ^ (value >> 31)) % upper_bound as u64) as usize
}

struct XorShift64(u64);

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_usize(&mut self, upper_bound: usize) -> usize {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value as usize % upper_bound
    }
}

fn directory_bytes(path: &Path) -> u64 {
    fs::read_dir(path)
        .expect("read benchmark directory")
        .map(|entry| {
            entry
                .expect("read benchmark directory entry")
                .metadata()
                .expect("read benchmark file metadata")
                .len()
        })
        .sum()
}

fn filesystem_type(path: &Path) -> String {
    let output = Command::new("stat")
        .args(["-f", "-c", "%T"])
        .arg(path)
        .output();
    match output {
        Ok(output) if output.status.success() => {
            String::from_utf8_lossy(&output.stdout).trim().to_string()
        }
        _ => "unknown".to_string(),
    }
}

fn command_version(program: &str, argument: &str) -> String {
    command_output(program, &[argument])
}

fn command_output(program: &str, arguments: &[&str]) -> String {
    Command::new(program)
        .args(arguments)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn cpu_model() -> String {
    fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|contents| {
            contents.lines().find_map(|line| {
                line.strip_prefix("model name\t:")
                    .or_else(|| line.strip_prefix("Model\t\t:"))
                    .map(str::trim)
                    .map(str::to_string)
            })
        })
        .unwrap_or_else(|| "unknown".to_string())
}

fn main() {
    let config = Config::parse();
    let run = RunDirectory::new(&config);
    let filesystem = filesystem_type(&run.path);
    let memory_backed = matches!(filesystem.as_str(), "tmpfs" | "ramfs");
    if memory_backed && !config.allow_memory_fs {
        panic!(
            "benchmark directory {} uses {filesystem}; choose an on-disk path with --dir, \
             or explicitly pass --allow-memory-fs for a CPU-only run",
            run.path.display()
        );
    }
    if filesystem == "unknown" {
        eprintln!(
            "warning: filesystem type could not be detected; verify that {} is on persistent storage",
            run.path.display()
        );
    }

    let workload = config.profile.workload();
    println!(
        "BENCH_ENV profile={} path={} filesystem={} rust={:?} git={} os={:?} cpu={:?} \
         logical_cpus={} value_bytes={} samples={}",
        config.profile.name(),
        run.path.display(),
        filesystem,
        command_version("rustc", "--version"),
        command_output("git", &["rev-parse", "HEAD"]),
        command_output("uname", &["-srmo"]),
        cpu_model(),
        std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get),
        VALUE_BYTES,
        workload.samples
    );
    println!(
        "BENCH_NOTE SSTable and recovery reads use a warm OS page cache; SSTable point reads \
         report explicit application-cache enabled/disabled scenarios; shared-runner results \
         are informational, not an SLA"
    );

    bench_writes(&run, workload);
    bench_reads(&run, workload);
    bench_recovery(&run, workload);
    bench_compaction(&run, workload);
    bench_tcp(&run, workload);
}
