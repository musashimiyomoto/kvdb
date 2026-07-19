//! kvdb-server — HTTP/REST server backed by the WAL-based [`Store`].
//!
//! Usage:
//!   kvdb-server [BIND_ADDR] [WAL_PATH]
//!
//! Defaults: BIND_ADDR=0.0.0.0:6380, WAL_PATH=kvdb.wal
//!
//! Credentials are read from the environment and are required:
//!   KVDB_USER, KVDB_PASSWORD
//!
//! See [`kvdb::http`] for the route table and auth behavior.

use std::process::ExitCode;

use kvdb::http::{AppState, router};
use kvdb::store::Store;
use kvdb::{log_error, log_info};
use tokio::net::TcpListener;

const TARGET: &str = "kvdb::server";

#[tokio::main]
async fn main() -> ExitCode {
    // Install the logger before anything else so startup is observable.
    kvdb::log::init();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            log_error!(TARGET, "fatal: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let bind_addr = args.next().unwrap_or_else(|| "0.0.0.0:6380".to_string());
    let wal_path = args.next().unwrap_or_else(|| "kvdb.wal".to_string());

    // Credentials are mandatory — refuse to start unauthenticated.
    let user = require_env("KVDB_USER")?;
    let password = require_env("KVDB_PASSWORD")?;

    let store = Store::open(&wal_path)?;
    log_info!(
        TARGET,
        "recovered {} key(s) from {}",
        store.len()?,
        store.wal_path().display()
    );

    let state = AppState::new(store, user, password);
    let app = router(state);

    let listener = TcpListener::bind(&bind_addr).await?;
    log_info!(TARGET, "listening on {bind_addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

/// Reads a required environment variable or returns a helpful error.
fn require_env(name: &str) -> Result<String, String> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => Ok(v),
        _ => Err(format!(
            "{name} must be set (export KVDB_USER and KVDB_PASSWORD before starting)"
        )),
    }
}
