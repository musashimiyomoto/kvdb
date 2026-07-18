//! Load / stress tests.
//!
//! These are heavier than the correctness suite and are marked `#[ignore]` so a
//! plain `cargo test` stays fast. Run them explicitly:
//!
//! ```sh
//! cargo test --release --test load -- --ignored --nocapture
//! ```
//!
//! They assert *correctness under volume and concurrency*, not timing — the
//! informational throughput numbers live in `tests/perf.rs`.

use std::path::PathBuf;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use kvdb::http::{AppState, router};
use kvdb::store::Store;
use tower::ServiceExt; // for `oneshot`

const USER: &str = "admin";
const PASS: &str = "secret";

fn tmp_dir(tag: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("kvdb-load-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// Inserts a large number of keys, forcing many flushes, then reopens the store
/// from disk only and verifies every logical value survived — exercising the
/// WAL-rotation + manifest + multi-SSTable read path at scale.
#[test]
#[ignore = "load test; run with --ignored"]
fn bulk_insert_survives_many_flushes_and_reopen() {
    let dir = tmp_dir("bulk");
    let wal = dir.join("kvdb.wal");

    const N: usize = 100_000;
    {
        let mut s = Store::open(&wal).unwrap();
        s.set_memtable_limit(1_000); // ~100 flushes
        s.set_compaction_threshold(0); // this test intentionally keeps every table
        for i in 0..N {
            s.set(key(i), value(i)).unwrap();
        }
        // Delete a quarter of the keys, spreading tombstones across tables.
        for i in (0..N).step_by(4) {
            s.delete(&key(i)).unwrap();
        }
        assert!(s.sstable_count() >= 90, "expected many flushed tables");
    }

    // Reopen from disk and verify the full logical state.
    let s = Store::open(&wal).unwrap();
    for i in 0..N {
        if i % 4 == 0 {
            assert_eq!(s.get(&key(i)), None, "key {i} should be deleted");
        } else {
            assert_eq!(s.get(&key(i)), Some(value(i)), "key {i} should survive");
        }
    }
    let expected_live = N - N.div_ceil(4);
    assert_eq!(s.len(), expected_live);

    std::fs::remove_dir_all(&dir).ok();
}

/// Fires many concurrent HTTP requests at a *shared* store (one `Arc<Mutex>`),
/// checking that the mutex serialization yields a correct final state with no
/// lost writes, panics, or poisoned locks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "load test; run with --ignored"]
async fn concurrent_http_writes_are_consistent() {
    let dir = tmp_dir("concurrent");
    let wal = dir.join("kvdb.wal");
    let store = Store::open(&wal).unwrap();
    let state = AppState::new(store, USER, PASS);

    const WRITERS: usize = 200;

    // Each task writes its own key concurrently.
    let mut handles = Vec::with_capacity(WRITERS);
    for i in 0..WRITERS {
        let st = state.clone();
        handles.push(tokio::spawn(async move {
            let uri = format!("/v1/keys/k{i}");
            let (status, _) = oneshot(&st, "PUT", &uri, &format!("v{i}")).await;
            assert_eq!(status, StatusCode::OK);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // Every write must be visible and correct.
    for i in 0..WRITERS {
        let (status, body) = oneshot(&state, "GET", &format!("/v1/keys/k{i}"), "").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, format!("v{i}"));
    }

    // Hammer a single key concurrently: the final value must be one of the
    // writers' values (last-writer-wins), and nothing may corrupt or poison.
    let mut handles = Vec::new();
    for i in 0..WRITERS {
        let st = state.clone();
        handles.push(tokio::spawn(async move {
            let (status, _) = oneshot(&st, "PUT", "/v1/keys/hot", &format!("w{i}")).await;
            assert_eq!(status, StatusCode::OK);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let (status, body) = oneshot(&state, "GET", "/v1/keys/hot", "").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        (0..WRITERS).any(|i| body == format!("w{i}")),
        "final value {body:?} must be one of the concurrent writes"
    );

    std::fs::remove_dir_all(&dir).ok();
}

// ---- helpers ---------------------------------------------------------------

fn key(i: usize) -> Vec<u8> {
    format!("key-{i:08}").into_bytes()
}

fn value(i: usize) -> Vec<u8> {
    format!("value-number-{i}").into_bytes()
}

/// Standard base64 of `user:pass` for the Basic auth header.
fn auth_header() -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let input = format!("{USER}:{PASS}");
    let input = input.as_bytes();
    let mut out = String::from("Basic ");
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[(n >> 18 & 63) as usize] as char);
        out.push(TABLE[(n >> 12 & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            TABLE[(n >> 6 & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// Sends one authenticated request through a router cloned from `state`.
async fn oneshot(state: &AppState, method: &str, uri: &str, body: &str) -> (StatusCode, String) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", auth_header())
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = router(state.clone()).oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}
