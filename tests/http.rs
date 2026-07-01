//! Integration tests for the HTTP layer.
//!
//! These drive the axum `Router` directly through `tower`'s `oneshot` — no
//! socket is bound, so the tests are fast and deterministic.

use std::path::PathBuf;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use kvdb::http::{AppState, router};
use kvdb::store::Store;
use tower::ServiceExt; // for `oneshot`

const USER: &str = "admin";
const PASS: &str = "secret";

fn tmp_path(tag: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("kvdb-http-test-{tag}-{}.wal", std::process::id()));
    let _ = std::fs::remove_file(&p);
    p
}

/// Builds fresh app state backed by a unique temp WAL.
fn state(tag: &str) -> (AppState, PathBuf) {
    let path = tmp_path(tag);
    let store = Store::open(&path).unwrap();
    (AppState::new(store, USER, PASS), path)
}

/// Encodes `user:pass` into a Basic auth header value.
fn basic(user: &str, pass: &str) -> String {
    format!(
        "Basic {}",
        base64_encode(format!("{user}:{pass}").as_bytes())
    )
}

/// Sends one request against a fresh router cloned from `state`.
async fn send(
    state: &AppState,
    method: &str,
    uri: &str,
    auth: Option<&str>,
    body: &str,
) -> (StatusCode, String) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(a) = auth {
        builder = builder.header("authorization", a);
    }
    let req = builder.body(Body::from(body.to_string())).unwrap();

    let resp = router(state.clone()).oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

#[tokio::test]
async fn health_needs_no_auth() {
    let (st, path) = state("health");
    let (status, body) = send(&st, "GET", "/health", None, "").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.trim(), "PONG");
    std::fs::remove_file(&path).ok();
}

#[tokio::test]
async fn rejects_missing_and_wrong_credentials() {
    let (st, path) = state("auth");

    let (status, _) = send(&st, "GET", "/v1/keys/foo", None, "").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let bad = basic(USER, "wrong");
    let (status, _) = send(&st, "GET", "/v1/keys/foo", Some(&bad), "").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    std::fs::remove_file(&path).ok();
}

#[tokio::test]
async fn put_get_delete_roundtrip() {
    let (st, path) = state("roundtrip");
    let auth = basic(USER, PASS);

    // Missing key first.
    let (status, _) = send(&st, "GET", "/v1/keys/city", Some(&auth), "").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Store it.
    let (status, body) = send(&st, "PUT", "/v1/keys/city", Some(&auth), "Berlin").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.trim(), "OK");

    // Read it back.
    let (status, body) = send(&st, "GET", "/v1/keys/city", Some(&auth), "").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "Berlin");

    // Delete it.
    let (status, _) = send(&st, "DELETE", "/v1/keys/city", Some(&auth), "").await;
    assert_eq!(status, StatusCode::OK);

    // Gone now.
    let (status, _) = send(&st, "GET", "/v1/keys/city", Some(&auth), "").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    std::fs::remove_file(&path).ok();
}

#[tokio::test]
async fn empty_body_stores_empty_value() {
    let (st, path) = state("empty-body");
    let auth = basic(USER, PASS);

    let (status, _) = send(&st, "PUT", "/v1/keys/blank", Some(&auth), "").await;
    assert_eq!(status, StatusCode::OK);

    // The key now exists with an empty value: 200 with an empty body, not 404.
    let (status, body) = send(&st, "GET", "/v1/keys/blank", Some(&auth), "").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "");

    std::fs::remove_file(&path).ok();
}

#[tokio::test]
async fn percent_encoded_key_roundtrips() {
    let (st, path) = state("pct-key");
    let auth = basic(USER, PASS);

    // Key "a/b c" arrives percent-encoded in the path and must be decoded to the
    // same bytes for storage and retrieval.
    let (status, _) = send(&st, "PUT", "/v1/keys/a%2Fb%20c", Some(&auth), "val").await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = send(&st, "GET", "/v1/keys/a%2Fb%20c", Some(&auth), "").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "val");

    std::fs::remove_file(&path).ok();
}

#[tokio::test]
async fn delete_absent_key_returns_404() {
    let (st, path) = state("http-del-absent");
    let auth = basic(USER, PASS);

    let (status, _) = send(&st, "DELETE", "/v1/keys/ghost", Some(&auth), "").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    std::fs::remove_file(&path).ok();
}

#[tokio::test]
async fn unsupported_method_is_405() {
    let (st, path) = state("http-405");
    let auth = basic(USER, PASS);

    // POST is not wired up for the key route; axum answers 405 for a known path
    // with an unknown method (auth still required, but a valid header passes it).
    let (status, _) = send(&st, "POST", "/v1/keys/x", Some(&auth), "y").await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);

    std::fs::remove_file(&path).ok();
}

#[tokio::test]
async fn large_body_roundtrips_over_http() {
    let (st, path) = state("http-large");
    let auth = basic(USER, PASS);

    let big = "x".repeat(512 * 1024); // 512 KiB
    let (status, _) = send(&st, "PUT", "/v1/keys/blob", Some(&auth), &big).await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = send(&st, "GET", "/v1/keys/blob", Some(&auth), "").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.len(), big.len());

    std::fs::remove_file(&path).ok();
}

/// Minimal standard base64 encoder (no padding shortcuts), for test auth headers.
fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
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
