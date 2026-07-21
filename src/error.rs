//! Structured errors returned by the storage engine.

use std::io;
use std::sync::Arc;

/// Coarse category for a [`StorageError`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageErrorKind {
    /// A caller supplied a key, value, batch, or sequence outside the contract.
    InvalidInput,
    /// Durable storage contains malformed or checksum-invalid data.
    Corruption,
    /// The operating system or backing filesystem could not complete an operation.
    Unavailable,
    /// An optimistic transaction observed a concurrent mutation.
    Conflict,
    /// A prior uncertain write made the current Store unsafe for further writes.
    Poisoned,
}

/// Failure returned by the public storage API.
///
/// The enum separates caller mistakes, durable-data corruption, environmental
/// I/O failures, transaction conflicts, and the Store's poisoned state so
/// callers do not need to infer semantics from an [`io::ErrorKind`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum StorageError {
    /// A caller supplied a value outside the documented storage contract.
    InvalidInput { message: String },
    /// A complete durable record or its metadata failed structural validation.
    Corruption { message: String },
    /// The backing filesystem or another required storage resource is unavailable.
    Unavailable { source: Arc<io::Error> },
    /// An optimistic transaction read or wrote a concurrently modified key.
    Conflict {
        key: Vec<u8>,
        expected: u64,
        actual: u64,
    },
    /// A prior uncertain write requires reopening the Store before more writes.
    Poisoned,
}

impl StorageError {
    /// Returns this failure's coarse semantic category.
    pub fn kind(&self) -> StorageErrorKind {
        match self {
            Self::InvalidInput { .. } => StorageErrorKind::InvalidInput,
            Self::Corruption { .. } => StorageErrorKind::Corruption,
            Self::Unavailable { .. } => StorageErrorKind::Unavailable,
            Self::Conflict { .. } => StorageErrorKind::Conflict,
            Self::Poisoned => StorageErrorKind::Poisoned,
        }
    }

    pub(crate) fn invalid_input(message: impl Into<String>) -> Self {
        Self::InvalidInput {
            message: message.into(),
        }
    }

    pub(crate) fn unavailable(error: io::Error) -> Self {
        Self::Unavailable {
            source: Arc::new(error),
        }
    }
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidInput { message } => f.write_str(message),
            Self::Corruption { message } => write!(f, "storage corruption: {message}"),
            Self::Unavailable { source } => source.fmt(f),
            Self::Conflict {
                key,
                expected,
                actual,
            } => write!(
                f,
                "transaction conflict for key {key:?}: expected at most sequence {expected}, actual {actual}"
            ),
            Self::Poisoned => f.write_str(
                "store is poisoned after an uncertain storage failure; reopen it before writing",
            ),
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Unavailable { source } => Some(source.as_ref()),
            _ => None,
        }
    }
}

impl From<io::Error> for StorageError {
    fn from(error: io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::InvalidInput => Self::InvalidInput {
                message: error.to_string(),
            },
            io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof => Self::Corruption {
                message: error.to_string(),
            },
            _ => Self::unavailable(error),
        }
    }
}

/// Result type returned by fallible public storage operations.
pub type StorageResult<T> = Result<T, StorageError>;
