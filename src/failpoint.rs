//! Opt-in crash failpoints for debug builds and recovery tests.

#[cfg(debug_assertions)]
pub(crate) fn hit(name: &str) {
    use std::sync::OnceLock;

    static SELECTED: OnceLock<Option<String>> = OnceLock::new();
    let selected = SELECTED.get_or_init(|| {
        (std::env::var("KVDB_ENABLE_FAILPOINTS").as_deref() == Ok("1"))
            .then(|| std::env::var("KVDB_FAILPOINT").ok())
            .flatten()
    });
    if selected.as_deref() == Some(name) {
        eprintln!("kvdb failpoint triggered: {name}");
        std::process::exit(86);
    }
}

#[cfg(not(debug_assertions))]
#[inline]
pub(crate) fn hit(_name: &str) {}
