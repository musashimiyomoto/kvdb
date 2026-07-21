//! Hard safety bounds shared by the storage codecs and public API.

/// Maximum key size accepted by the storage engine (1 MiB).
pub const MAX_KEY_BYTES: usize = 1024 * 1024;
/// Maximum value size accepted by the storage engine (64 MiB).
pub const MAX_VALUE_BYTES: usize = 64 * 1024 * 1024;
/// Maximum number of operations in one atomic batch.
pub const MAX_BATCH_OPERATIONS: usize = 100_000;

pub(crate) const MAX_WAL_RECORD_BYTES: usize = 128 * 1024 * 1024;
pub(crate) const MAX_VERSIONS_PER_KEY: usize = 65_536;
pub(crate) const MAX_SSTABLE_RECORD_BYTES: usize = 256 * 1024 * 1024;
pub(crate) const MAX_BLOOM_FILTER_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_BLOOM_HASHES: u8 = 32;
pub(crate) const MAX_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;
pub(crate) const MAX_MANIFEST_LINE_BYTES: usize = 4 * 1024;
pub(crate) const MAX_MANIFEST_SSTABLES: usize = 10_000;
