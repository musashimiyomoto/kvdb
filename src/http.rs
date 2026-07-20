//! HTTP/REST layer: an axum router backed by one bounded storage worker and
//! protected by HTTP Basic Auth.
//!
//! Routes:
//! ```text
//!   GET    /health           -> 200 "PONG"      (no auth)
//!   GET    /v1/keys/{key}    -> 200 <value> | 404
//!   PUT    /v1/keys/{key}    -> 200 "OK"        (body is the value)
//!   DELETE /v1/keys/{key}    -> 200 "OK" | 404
//! ```

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Router, extract::Request};
use axum_extra::TypedHeader;
use axum_extra::headers::{Authorization, authorization::Basic};
use tokio::sync::{mpsc, oneshot};

use crate::log_error;
use crate::store::{Durability, Store, WriteBatch};

const TARGET: &str = "kvdb::http";
const DEFAULT_QUEUE_CAPACITY: usize = 1_024;
const DEFAULT_GROUP_COMMIT_MAX: usize = 64;
const DEFAULT_GROUP_COMMIT_DELAY: Duration = Duration::from_millis(1);

/// Queue and group-commit settings for the HTTP storage worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageOptions {
    pub queue_capacity: usize,
    pub group_commit_max: usize,
    pub group_commit_delay: Duration,
}

impl Default for StorageOptions {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            group_commit_max: DEFAULT_GROUP_COMMIT_MAX,
            group_commit_delay: DEFAULT_GROUP_COMMIT_DELAY,
        }
    }
}

impl StorageOptions {
    fn from_env() -> Self {
        let defaults = Self::default();
        Self {
            queue_capacity: positive_usize_env("KVDB_STORAGE_QUEUE_CAPACITY")
                .unwrap_or(defaults.queue_capacity),
            group_commit_max: positive_usize_env("KVDB_GROUP_COMMIT_MAX")
                .unwrap_or(defaults.group_commit_max),
            group_commit_delay: std::env::var("KVDB_GROUP_COMMIT_DELAY_US")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_micros)
                .unwrap_or(defaults.group_commit_delay),
        }
    }

    fn normalized(self) -> Self {
        Self {
            queue_capacity: self.queue_capacity.max(1),
            group_commit_max: self.group_commit_max.max(1),
            group_commit_delay: self.group_commit_delay,
        }
    }
}

/// A point-in-time view of storage-worker grouping and overload counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageMetrics {
    pub write_groups: u64,
    pub logical_writes: u64,
    pub max_group_size: usize,
    pub queue_full: u64,
}

#[derive(Default)]
struct StorageMetricsInner {
    write_groups: AtomicU64,
    logical_writes: AtomicU64,
    max_group_size: AtomicUsize,
    queue_full: AtomicU64,
}

impl StorageMetricsInner {
    fn snapshot(&self) -> StorageMetrics {
        StorageMetrics {
            write_groups: self.write_groups.load(Ordering::Relaxed),
            logical_writes: self.logical_writes.load(Ordering::Relaxed),
            max_group_size: self.max_group_size.load(Ordering::Relaxed),
            queue_full: self.queue_full.load(Ordering::Relaxed),
        }
    }

    fn record_group(&self, size: usize) {
        self.write_groups.fetch_add(1, Ordering::Relaxed);
        self.logical_writes
            .fetch_add(size as u64, Ordering::Relaxed);
        self.max_group_size.fetch_max(size, Ordering::Relaxed);
    }
}

/// Shared state handed to every request handler.
#[derive(Clone)]
pub struct AppState {
    storage: StorageHandle,
    user: Arc<str>,
    password: Arc<str>,
}

impl AppState {
    /// Builds state with environment-configured storage-worker settings.
    pub fn new(store: Store, user: impl Into<String>, password: impl Into<String>) -> Self {
        Self::with_storage_options(store, user, password, StorageOptions::from_env())
    }

    /// Builds state with explicit settings, primarily for controlled deployment
    /// configuration, tests, and benchmarks.
    pub fn with_storage_options(
        store: Store,
        user: impl Into<String>,
        password: impl Into<String>,
        options: StorageOptions,
    ) -> Self {
        Self {
            storage: StorageHandle::spawn(store, options.normalized()),
            user: Arc::from(user.into()),
            password: Arc::from(password.into()),
        }
    }

    pub fn storage_metrics(&self) -> StorageMetrics {
        self.storage.metrics.snapshot()
    }
}

#[derive(Clone)]
struct StorageHandle {
    sender: mpsc::Sender<StorageCommand>,
    metrics: Arc<StorageMetricsInner>,
}

impl StorageHandle {
    fn spawn(store: Store, options: StorageOptions) -> Self {
        let (sender, receiver) = mpsc::channel(options.queue_capacity);
        let metrics = Arc::new(StorageMetricsInner::default());
        let worker_metrics = Arc::clone(&metrics);
        std::thread::Builder::new()
            .name("kvdb-storage".to_string())
            .spawn(move || storage_worker(store, receiver, options, worker_metrics))
            .expect("failed to spawn kvdb storage worker");
        Self { sender, metrics }
    }

    async fn get(&self, key: Vec<u8>) -> Result<io::Result<Option<Vec<u8>>>, DispatchError> {
        let (response, receiver) = oneshot::channel();
        self.enqueue(StorageCommand::Get { key, response })?;
        receiver.await.map_err(|_| DispatchError::Closed)
    }

    async fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<io::Result<()>, DispatchError> {
        let (response, receiver) = oneshot::channel();
        self.enqueue(StorageCommand::Write(WriteCommand::Set {
            key,
            value,
            response,
        }))?;
        receiver.await.map_err(|_| DispatchError::Closed)
    }

    async fn delete(&self, key: Vec<u8>) -> Result<io::Result<bool>, DispatchError> {
        let (response, receiver) = oneshot::channel();
        self.enqueue(StorageCommand::Write(WriteCommand::Delete {
            key,
            response,
        }))?;
        receiver.await.map_err(|_| DispatchError::Closed)
    }

    fn enqueue(&self, command: StorageCommand) -> Result<(), DispatchError> {
        self.sender.try_send(command).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => {
                self.metrics.queue_full.fetch_add(1, Ordering::Relaxed);
                DispatchError::Full
            }
            mpsc::error::TrySendError::Closed(_) => DispatchError::Closed,
        })
    }
}

enum StorageCommand {
    Get {
        key: Vec<u8>,
        response: oneshot::Sender<io::Result<Option<Vec<u8>>>>,
    },
    Write(WriteCommand),
}

enum WriteCommand {
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        response: oneshot::Sender<io::Result<()>>,
    },
    Delete {
        key: Vec<u8>,
        response: oneshot::Sender<io::Result<bool>>,
    },
}

enum WriteReply {
    Set(oneshot::Sender<io::Result<()>>),
    Delete {
        existed: bool,
        response: oneshot::Sender<io::Result<bool>>,
    },
}

#[derive(Clone, Copy, Debug)]
enum DispatchError {
    Full,
    Closed,
}

fn storage_worker(
    mut store: Store,
    mut receiver: mpsc::Receiver<StorageCommand>,
    options: StorageOptions,
    metrics: Arc<StorageMetricsInner>,
) {
    let mut pending = None;
    loop {
        if let Err(error) = store.poll_background_compaction() {
            log_error!(TARGET, "background compaction publication failed: {error}");
        }
        let command = match pending.take().or_else(|| receiver.blocking_recv()) {
            Some(command) => command,
            None => break,
        };

        match command {
            StorageCommand::Get { key, response } => {
                let _ = response.send(store.get(&key));
            }
            StorageCommand::Write(first) => {
                let mut group = vec![first];
                drain_adjacent_writes(
                    &mut receiver,
                    &mut pending,
                    &mut group,
                    options.group_commit_max,
                );
                if group.len() < options.group_commit_max
                    && pending.is_none()
                    && store.durability() == Durability::Durable
                    && !options.group_commit_delay.is_zero()
                {
                    std::thread::sleep(options.group_commit_delay);
                    drain_adjacent_writes(
                        &mut receiver,
                        &mut pending,
                        &mut group,
                        options.group_commit_max,
                    );
                }
                metrics.record_group(group.len());
                process_write_group(&mut store, group);
            }
        }
    }
}

fn drain_adjacent_writes(
    receiver: &mut mpsc::Receiver<StorageCommand>,
    pending: &mut Option<StorageCommand>,
    group: &mut Vec<WriteCommand>,
    max_group_size: usize,
) {
    while group.len() < max_group_size {
        match receiver.try_recv() {
            Ok(StorageCommand::Write(write)) => group.push(write),
            Ok(command) => {
                *pending = Some(command);
                break;
            }
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                break;
            }
        }
    }
}

fn process_write_group(store: &mut Store, writes: Vec<WriteCommand>) {
    let mut overlay = BTreeMap::<Vec<u8>, bool>::new();
    let mut delete_results = Vec::with_capacity(writes.len());

    for write in &writes {
        match write {
            WriteCommand::Set { key, .. } => {
                overlay.insert(key.clone(), true);
                delete_results.push(None);
            }
            WriteCommand::Delete { key, .. } => {
                let existed = match overlay.get(key) {
                    Some(existed) => *existed,
                    None => match store.get(key) {
                        Ok(value) => value.is_some(),
                        Err(error) => {
                            fail_commands(writes, &error);
                            return;
                        }
                    },
                };
                overlay.insert(key.clone(), false);
                delete_results.push(Some(existed));
            }
        }
    }

    let mut batches = Vec::with_capacity(writes.len());
    let mut replies = Vec::with_capacity(writes.len());
    for (write, delete_existed) in writes.into_iter().zip(delete_results) {
        match write {
            WriteCommand::Set {
                key,
                value,
                response,
            } => {
                overlay.insert(key.clone(), true);
                let mut batch = WriteBatch::new();
                batch.set(key, value);
                batches.push(batch);
                replies.push(WriteReply::Set(response));
            }
            WriteCommand::Delete { key, response } => {
                let mut batch = WriteBatch::new();
                batch.delete(key);
                batches.push(batch);
                replies.push(WriteReply::Delete {
                    existed: delete_existed.expect("DELETE preflight result"),
                    response,
                });
            }
        }
    }

    match store.write_group(batches) {
        Ok(_) => {
            for reply in replies {
                match reply {
                    WriteReply::Set(response) => {
                        let _ = response.send(Ok(()));
                    }
                    WriteReply::Delete { existed, response } => {
                        let _ = response.send(Ok(existed));
                    }
                }
            }
        }
        Err(error) => fail_replies(replies, &error),
    }
}

fn fail_commands(commands: Vec<WriteCommand>, error: &io::Error) {
    for command in commands {
        match command {
            WriteCommand::Set { response, .. } => {
                let _ = response.send(Err(copy_io_error(error)));
            }
            WriteCommand::Delete { response, .. } => {
                let _ = response.send(Err(copy_io_error(error)));
            }
        }
    }
}

fn fail_replies(replies: Vec<WriteReply>, error: &io::Error) {
    for reply in replies {
        match reply {
            WriteReply::Set(response) => {
                let _ = response.send(Err(copy_io_error(error)));
            }
            WriteReply::Delete { response, .. } => {
                let _ = response.send(Err(copy_io_error(error)));
            }
        }
    }
}

fn copy_io_error(error: &io::Error) -> io::Error {
    io::Error::new(error.kind(), error.to_string())
}

/// Constructs the application router with all routes and the auth layer.
pub fn router(state: AppState) -> Router {
    let protected = Router::new()
        .route(
            "/v1/keys/{key}",
            get(get_key).put(put_key).delete(delete_key),
        )
        .route_layer(middleware::from_fn_with_state(state.clone(), auth));

    Router::new()
        .route("/health", get(health))
        .merge(protected)
        .with_state(state)
}

async fn health() -> &'static str {
    "PONG"
}

async fn get_key(State(state): State<AppState>, Path(key): Path<String>) -> Response {
    match state.storage.get(key.into_bytes()).await {
        Ok(Ok(Some(value))) => (StatusCode::OK, value).into_response(),
        Ok(Ok(None)) => (StatusCode::NOT_FOUND, "not found\n").into_response(),
        Ok(Err(error)) => {
            log_error!(TARGET, "get failed: {error}");
            storage_error()
        }
        Err(error) => dispatch_error(error),
    }
}

async fn put_key(State(state): State<AppState>, Path(key): Path<String>, body: Bytes) -> Response {
    match state.storage.set(key.into_bytes(), body.to_vec()).await {
        Ok(Ok(())) => (StatusCode::OK, "OK\n").into_response(),
        Ok(Err(error)) => {
            log_error!(TARGET, "set failed: {error}");
            storage_error()
        }
        Err(error) => dispatch_error(error),
    }
}

async fn delete_key(State(state): State<AppState>, Path(key): Path<String>) -> Response {
    match state.storage.delete(key.into_bytes()).await {
        Ok(Ok(true)) => (StatusCode::OK, "OK\n").into_response(),
        Ok(Ok(false)) => (StatusCode::NOT_FOUND, "not found\n").into_response(),
        Ok(Err(error)) => {
            log_error!(TARGET, "delete failed: {error}");
            storage_error()
        }
        Err(error) => dispatch_error(error),
    }
}

async fn auth(
    State(state): State<AppState>,
    creds: Option<TypedHeader<Authorization<Basic>>>,
    request: Request,
    next: Next,
) -> Response {
    let ok = match &creds {
        Some(TypedHeader(auth)) => {
            constant_time_eq(auth.username().as_bytes(), state.user.as_bytes())
                & constant_time_eq(auth.password().as_bytes(), state.password.as_bytes())
        }
        None => false,
    };

    if ok {
        next.run(request).await
    } else {
        unauthorized()
    }
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Basic realm=\"kvdb\"")],
        "unauthorized\n",
    )
        .into_response()
}

fn dispatch_error(error: DispatchError) -> Response {
    match error {
        DispatchError::Full => {
            (StatusCode::SERVICE_UNAVAILABLE, "storage queue full\n").into_response()
        }
        DispatchError::Closed => storage_error(),
    }
}

fn storage_error() -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, "storage unavailable\n").into_response()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn positive_usize_env(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&value| value > 0)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn test_store(tag: &str) -> (Store, PathBuf) {
        let path =
            std::env::temp_dir().join(format!("kvdb-http-worker-{tag}-{}.wal", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("wal.lock"));
        (Store::open(&path).unwrap(), path)
    }

    fn options(queue_capacity: usize, delay: Duration) -> StorageOptions {
        StorageOptions {
            queue_capacity,
            group_commit_max: 16,
            group_commit_delay: delay,
        }
    }

    async fn cleanup(storage: StorageHandle, path: PathBuf) {
        drop(storage);
        tokio::time::sleep(Duration::from_millis(5)).await;
        std::fs::remove_file(path.with_extension("wal.lock")).ok();
        std::fs::remove_file(path).ok();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_groups_adjacent_writes_and_preserves_delete_semantics() {
        let (store, path) = test_store("group");
        let storage = StorageHandle::spawn(store, options(16, Duration::from_millis(20)));

        let (set, first_delete, second_delete) = tokio::join!(
            storage.set(b"key".to_vec(), b"value".to_vec()),
            storage.delete(b"key".to_vec()),
            storage.delete(b"key".to_vec()),
        );
        assert!(set.unwrap().is_ok());
        assert!(first_delete.unwrap().unwrap());
        assert!(!second_delete.unwrap().unwrap());
        assert_eq!(storage.get(b"key".to_vec()).await.unwrap().unwrap(), None);

        let metrics = storage.metrics.snapshot();
        assert_eq!(metrics.write_groups, 1);
        assert_eq!(metrics.logical_writes, 3);
        assert_eq!(metrics.max_group_size, 3);

        cleanup(storage, path).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn full_queue_is_rejected_without_blocking_the_async_caller() {
        let (store, path) = test_store("overload");
        let storage = StorageHandle::spawn(store, options(1, Duration::from_millis(100)));
        let first_storage = storage.clone();
        let first = tokio::spawn(async move {
            first_storage
                .set(b"first".to_vec(), b"value".to_vec())
                .await
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        let (queued_response, queued_receiver) = oneshot::channel();
        storage
            .enqueue(StorageCommand::Get {
                key: b"first".to_vec(),
                response: queued_response,
            })
            .unwrap();
        let (rejected_response, _rejected_receiver) = oneshot::channel();
        assert!(matches!(
            storage.enqueue(StorageCommand::Get {
                key: b"other".to_vec(),
                response: rejected_response,
            }),
            Err(DispatchError::Full)
        ));

        assert!(first.await.unwrap().unwrap().is_ok());
        assert_eq!(
            queued_receiver.await.unwrap().unwrap(),
            Some(b"value".to_vec())
        );
        assert_eq!(storage.metrics.snapshot().queue_full, 1);

        cleanup(storage, path).await;
    }
}
