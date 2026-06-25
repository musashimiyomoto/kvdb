//! kvdb — a small networked key-value database with an HTTP/REST API.
//!
//! * [`store`] holds the LSM-style storage engine (memtable + write-ahead log).
//! * [`http`] exposes the store over HTTP with Basic-auth protected routes.
//!
//! The `kvdb-server` and `kvdb-client` binaries build on these two modules.

pub mod http;
pub mod store;

pub use http::{AppState, router};
pub use store::Store;
