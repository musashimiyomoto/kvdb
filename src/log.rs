//! A tiny dependency-free logger that writes to the console **and**, optionally,
//! to a file.
//!
//! In the same spirit as the rest of this project (hand-rolled base64, a custom
//! WAL format, `reqwest` without TLS) this avoids pulling in `log` / `tracing`
//! and their subscriber stacks. It is deliberately small:
//!
//! * four levels — [`Level::Error`] > `Warn` > `Info` > `Debug`;
//! * a single global sink installed once via [`init`], guarded by a `Mutex`;
//! * every record goes to `stderr`, and additionally to a file when
//!   `KVDB_LOG_FILE` is set;
//! * UTC RFC3339 timestamps computed by hand (no `chrono`).
//!
//! Use the [`log_error!`], [`log_warn!`], [`log_info!`] and [`log_debug!`]
//! macros. Each takes an explicit `target` string first (conventionally the
//! module, e.g. `"kvdb::store"`), then a format string:
//!
//! ```
//! use kvdb::log_info;
//! log_info!("kvdb::demo", "recovered {} key(s)", 3);
//! ```
//!
//! Logging is best-effort: a failed write is swallowed rather than allowed to
//! crash a request or a flush.

use std::fmt;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// Severity of a log record. Ordered so that a configured minimum level admits
/// everything at least as severe (`Error` is the most severe / smallest set).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Error,
    Warn,
    Info,
    Debug,
}

impl Level {
    /// Fixed-width label used in the log line (keeps columns aligned).
    fn label(self) -> &'static str {
        match self {
            Level::Error => "ERROR",
            Level::Warn => "WARN ",
            Level::Info => "INFO ",
            Level::Debug => "DEBUG",
        }
    }

    /// Parses a level name case-insensitively; unknown names yield `None`.
    fn parse(s: &str) -> Option<Level> {
        match s.trim().to_ascii_lowercase().as_str() {
            "error" => Some(Level::Error),
            "warn" | "warning" => Some(Level::Warn),
            "info" => Some(Level::Info),
            "debug" => Some(Level::Debug),
            _ => None,
        }
    }
}

/// The installed logger: a maximum level plus an optional file sink.
struct Logger {
    max: Level,
    file: Mutex<Option<std::fs::File>>,
}

/// The one global logger. `None` until [`init`] runs; logging before then is a
/// no-op so library code and tests don't depend on initialization.
static LOGGER: OnceLock<Logger> = OnceLock::new();

/// Installs the global logger from the environment. Idempotent: the first call
/// wins and later calls are ignored (returns `false` if already initialized).
///
/// * `KVDB_LOG` — minimum level (`error`/`warn`/`info`/`debug`); default `info`.
/// * `KVDB_LOG_FILE` — if set and non-empty, log lines are appended here in
///   addition to `stderr`. A file that cannot be opened is reported once to
///   `stderr` and logging continues console-only.
pub fn init() -> bool {
    let max = std::env::var("KVDB_LOG")
        .ok()
        .and_then(|v| Level::parse(&v))
        .unwrap_or(Level::Info);

    let file = match std::env::var("KVDB_LOG_FILE") {
        Ok(path) if !path.is_empty() => {
            match OpenOptions::new().create(true).append(true).open(&path) {
                Ok(f) => Some(f),
                Err(e) => {
                    eprintln!(
                        "kvdb: cannot open KVDB_LOG_FILE {path:?}: {e} (logging to stderr only)"
                    );
                    None
                }
            }
        }
        _ => None,
    };

    LOGGER
        .set(Logger {
            max,
            file: Mutex::new(file),
        })
        .is_ok()
}

/// Emits one record. Called by the `log_*!` macros; prefer those.
///
/// A record below the configured level, or emitted before [`init`], is dropped.
/// Formatting only happens once we know the record will be written.
pub fn log(level: Level, target: &str, args: fmt::Arguments) {
    let Some(logger) = LOGGER.get() else { return };
    if level > logger.max {
        return;
    }

    // `[timestamp LEVEL target] message`
    let line = format!(
        "[{} {} {}] {}\n",
        now_rfc3339(),
        level.label(),
        target,
        args
    );

    // Console sink: always. Best-effort — never unwrap on a logging write.
    let _ = io::stderr().write_all(line.as_bytes());

    // File sink: only when configured. Hold the lock just for the write.
    if let Ok(mut guard) = logger.file.lock()
        && let Some(file) = guard.as_mut()
    {
        let _ = file.write_all(line.as_bytes());
    }
}

/// Formats the current time as a UTC RFC3339 timestamp, e.g.
/// `2026-07-01T12:00:00Z`. Computed by hand to avoid a date/time dependency.
///
/// A clock reading before the Unix epoch (shouldn't happen) degrades to the
/// epoch rather than panicking.
fn now_rfc3339() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (hour, min, sec) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (year, month, day) = civil_from_days(days);

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
}

/// Converts a count of days since 1970-01-01 into a `(year, month, day)` triple
/// in the proleptic Gregorian calendar.
///
/// This is Howard Hinnant's `civil_from_days` algorithm (public domain), the
/// same one the C++ `<chrono>` calendar is built on.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (year + if month <= 2 { 1 } else { 0 }, month, day)
}

/// Logs at [`Level::Error`]. `log_error!(target, "fmt", args...)`.
#[macro_export]
macro_rules! log_error {
    ($target:expr, $($arg:tt)*) => {
        $crate::log::log($crate::log::Level::Error, $target, format_args!($($arg)*))
    };
}

/// Logs at [`Level::Warn`].
#[macro_export]
macro_rules! log_warn {
    ($target:expr, $($arg:tt)*) => {
        $crate::log::log($crate::log::Level::Warn, $target, format_args!($($arg)*))
    };
}

/// Logs at [`Level::Info`].
#[macro_export]
macro_rules! log_info {
    ($target:expr, $($arg:tt)*) => {
        $crate::log::log($crate::log::Level::Info, $target, format_args!($($arg)*))
    };
}

/// Logs at [`Level::Debug`].
#[macro_export]
macro_rules! log_debug {
    ($target:expr, $($arg:tt)*) => {
        $crate::log::log($crate::log::Level::Debug, $target, format_args!($($arg)*))
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levels_are_ordered_by_severity() {
        assert!(Level::Error < Level::Warn);
        assert!(Level::Warn < Level::Info);
        assert!(Level::Info < Level::Debug);
    }

    #[test]
    fn parses_level_names() {
        assert!(matches!(Level::parse("ERROR"), Some(Level::Error)));
        assert!(matches!(Level::parse("warning"), Some(Level::Warn)));
        assert!(matches!(Level::parse(" info "), Some(Level::Info)));
        assert!(Level::parse("nonsense").is_none());
    }

    #[test]
    fn civil_from_days_matches_known_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        // 2000-01-01 is 10957 days after the epoch; 2000 is a leap year.
        assert_eq!(civil_from_days(10_957), (2000, 1, 1));
        assert_eq!(civil_from_days(10_957 + 59), (2000, 2, 29));
    }
}
