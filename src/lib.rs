//! kvdb — a small networked key-value database with an HTTP/REST API.
//!
//! * [`store`] holds the LSM-style storage engine (memtable + write-ahead log).
//! * [`http`] exposes the store over HTTP with Basic-auth protected routes.
//! * [`log`] is a small dependency-free console+file logger.
//!
//! The `kvdb-server` and `kvdb-client` binaries build on these modules.

pub mod http;
pub mod log;
pub mod sstable;
pub mod store;

pub use http::{AppState, router};
pub use store::{BatchOperation, Snapshot, Store, WriteBatch};
