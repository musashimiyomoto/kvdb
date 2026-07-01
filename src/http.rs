//! HTTP/REST layer: an axum router over the [`Store`], protected by HTTP Basic
//! Auth.
//!
//! Routes:
//! ```text
//!   GET    /health           -> 200 "PONG"      (no auth)
//!   GET    /v1/keys/{key}     -> 200 <value> | 404
//!   PUT    /v1/keys/{key}     -> 200 "OK"        (body is the value)
//!   DELETE /v1/keys/{key}     -> 200 "OK" | 404
//! ```
//! Every `/v1/*` route requires a valid `Authorization: Basic` header matching
//! the configured credentials; otherwise it returns `401` with a
//! `WWW-Authenticate: Basic` challenge.
//!
//! The router is built here (rather than in the server binary) so integration
//! tests can exercise it directly via `tower`'s `oneshot`.

use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Router, extract::Request};
use axum_extra::TypedHeader;
use axum_extra::headers::{Authorization, authorization::Basic};

use crate::store::Store;

/// Shared state handed to every request handler.
#[derive(Clone)]
pub struct AppState {
    store: Arc<Mutex<Store>>,
    user: Arc<str>,
    password: Arc<str>,
}

impl AppState {
    /// Builds state from an opened store and the expected credentials.
    pub fn new(store: Store, user: impl Into<String>, password: impl Into<String>) -> Self {
        AppState {
            store: Arc::new(Mutex::new(store)),
            user: Arc::from(user.into()),
            password: Arc::from(password.into()),
        }
    }
}

/// Constructs the application router with all routes and the auth layer.
pub fn router(state: AppState) -> Router {
    // Routes that require authentication.
    let protected = Router::new()
        .route(
            "/v1/keys/{key}",
            get(get_key).put(put_key).delete(delete_key),
        )
        .route_layer(middleware::from_fn_with_state(state.clone(), auth));

    // `/health` is intentionally public so containers/load balancers can probe it.
    Router::new()
        .route("/health", get(health))
        .merge(protected)
        .with_state(state)
}

/// Liveness probe. Doubles as the client's `PING`.
async fn health() -> &'static str {
    "PONG"
}

/// `GET /v1/keys/{key}` — returns the raw value bytes, or 404.
async fn get_key(State(state): State<AppState>, Path(key): Path<String>) -> Response {
    let value = {
        let store = match state.store.lock() {
            Ok(g) => g,
            Err(_) => return lock_error(),
        };
        store.get(key.as_bytes())
    };
    match value {
        Some(v) => (StatusCode::OK, v).into_response(),
        None => (StatusCode::NOT_FOUND, "not found\n").into_response(),
    }
}

/// `PUT /v1/keys/{key}` — stores the request body as the value.
async fn put_key(State(state): State<AppState>, Path(key): Path<String>, body: Bytes) -> Response {
    let mut store = match state.store.lock() {
        Ok(g) => g,
        Err(_) => return lock_error(),
    };
    match store.set(key.into_bytes(), body.to_vec()) {
        Ok(()) => (StatusCode::OK, "OK\n").into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("set failed: {e}\n"),
        )
            .into_response(),
    }
}

/// `DELETE /v1/keys/{key}` — removes a key; 404 if it did not exist.
async fn delete_key(State(state): State<AppState>, Path(key): Path<String>) -> Response {
    let mut store = match state.store.lock() {
        Ok(g) => g,
        Err(_) => return lock_error(),
    };
    match store.delete(key.as_bytes()) {
        Ok(true) => (StatusCode::OK, "OK\n").into_response(),
        Ok(false) => (StatusCode::NOT_FOUND, "not found\n").into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("delete failed: {e}\n"),
        )
            .into_response(),
    }
}

/// Auth middleware: validates HTTP Basic credentials against `AppState`.
///
/// The credential comparison is constant-time to avoid leaking how much of the
/// username/password matched via response timing.
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

/// `401` with a Basic-auth challenge so clients know to send credentials.
fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Basic realm=\"kvdb\"")],
        "unauthorized\n",
    )
        .into_response()
}

/// `500` used when the shared store mutex is poisoned by a prior panic.
fn lock_error() -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, "store lock poisoned\n").into_response()
}

/// Compares two byte slices without short-circuiting on the first difference.
///
/// Returns `true` iff equal. Length is compared first (lengths are not secret).
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
