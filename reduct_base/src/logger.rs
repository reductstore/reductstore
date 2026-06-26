// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use chrono::prelude::{DateTime, Utc};
use log::{Level, Log, Metadata, Record};
use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use thread_id;

static LOGGER: Logger = Logger;

pub struct Logger;
static PATHS: LazyLock<RwLock<BTreeMap<String, Level>>> =
    LazyLock::new(|| RwLock::new(BTreeMap::new()));

/// An owned, `Send + 'static` snapshot of a single log record.
///
/// `log::Record` borrows its fields and is neither `Send` nor `'static`, so a
/// sink that wants to process records off-thread must copy what it needs
/// synchronously. This struct uses only types available to `reduct_base`
/// (`log::Level` is from the `log` crate), keeping the hook fully decoupled from
/// any consumer.
#[derive(Clone, Debug)]
pub struct LogSnapshot {
    /// Microseconds since the Unix epoch, captured when the record was logged.
    pub timestamp: u64,
    pub level: Level,
    pub target: String,
    pub file: Option<String>,
    pub line: Option<u32>,
    pub message: String,
}

impl LogSnapshot {
    fn from_record(record: &Record) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        LogSnapshot {
            timestamp,
            level: record.level(),
            target: record.target().to_string(),
            file: record.file().map(|file| file.to_string()),
            line: record.line(),
            message: format!("{}", record.args()),
        }
    }
}

/// A registered log sink: receives an owned [`LogSnapshot`] for every record
/// that reaches the logger. It must be non-blocking and must not panic.
pub type LogSink = Arc<dyn Fn(LogSnapshot) + Send + Sync>;

static LOG_SINK: RwLock<Option<LogSink>> = RwLock::new(None);

/// Register (or replace) the global log sink. Called once at startup by the
/// component that persists logs; `reduct_base` itself never inspects the sink.
pub fn set_log_sink(sink: LogSink) {
    *LOG_SINK.write().unwrap() = Some(sink);
}

/// Remove the global log sink (used on shutdown and in tests).
pub fn clear_log_sink() {
    *LOG_SINK.write().unwrap() = None;
}

/// Parse a single log level name (case-insensitive) into a [`Level`].
///
/// Returns `None` for unset / `OFF` / invalid input, which callers treat as
/// "disabled". Unlike [`Logger::init`] this parses exactly one level and does
/// not mutate any global logger state.
pub fn parse_log_level(level: &str) -> Option<Level> {
    match level.trim().to_uppercase().as_str() {
        "ERROR" => Some(Level::Error),
        "WARN" => Some(Level::Warn),
        "INFO" => Some(Level::Info),
        "DEBUG" => Some(Level::Debug),
        "TRACE" => Some(Level::Trace),
        _ => None,
    }
}
impl Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        let paths = PATHS.read().unwrap();
        // Check paths in reverse order (most specific first)
        for (path, level) in paths.iter().rev() {
            if path.is_empty() {
                return metadata.level() <= *level;
            }
            if metadata.target().replace("::", "/").starts_with(path) {
                return metadata.level() <= *level;
            }
        }
        false
    }

    fn log(&self, record: &Record) {
        // Console output is ALWAYS produced here (subject only to the existing
        // level filter); it is never gated by the capture hook below. This
        // guarantees a log emitted *during* a capture write still reaches stdout.
        if self.enabled(record.metadata()) {
            let now: DateTime<Utc> = Utc::now();

            let file = if let Some(file) = record.file() {
                // Absolute path to crate, remove path to registry
                match file.split_once("src/") {
                    Some((_, file)) => file,
                    None => file,
                }
            } else {
                "(unknown)"
            };

            let package_name = if let Some(package_name) = record.target().split_once(':') {
                package_name.0
            } else {
                record.target()
            };

            let formatted = format!(
                "{} ({:>5}) [{}] -- {:}/{:}:{:} {:?}",
                now.format("%Y-%m-%d %H:%M:%S.%3f"),
                thread_id::get() % 100000,
                record.level(),
                package_name,
                file,
                record.line().unwrap(),
                record.args(),
            );
            println!("{}", formatted);
            #[cfg(test)]
            test_support::record_console(formatted);
        }

        // Optional capture hook (e.g. persisting logs to storage). It runs
        // AFTER console output and is fully abstract: this crate knows nothing
        // about what the sink does. Any reentrancy guarding lives inside the
        // registered sink, never here, so console output is never suppressed.
        let sink = LOG_SINK.read().unwrap().clone();
        if let Some(sink) = sink {
            sink(LogSnapshot::from_record(record));
        }
    }

    fn flush(&self) {}
}

impl Logger {
    /// Initialize the logger.
    ///
    /// # Arguments
    ///
    /// * `level` - The log level to use. Can be one of TRACE, DEBUG, INFO, WARN, ERROR.
    pub fn init(levels: &str) {
        let mut max_level = Level::Trace;
        {
            let mut paths = PATHS.write().unwrap();
            paths.clear();
            paths.insert("".to_string(), Level::Info); // default level
        }
        for level in levels.split(',') {
            let mut parts = level.splitn(2, '=');
            let mut path = parts.next().unwrap().trim();
            let level = if let Some(lvl) = parts.next() {
                lvl.trim()
            } else {
                // for case INFO,path=DEBUG
                let lvl = path;
                path = ""; // root
                lvl
            };

            let level = match level.to_uppercase().as_str() {
                "TRACE" => Level::Trace,
                "DEBUG" => Level::Debug,
                "INFO" => Level::Info,
                "WARN" => Level::Warn,
                "ERROR" => Level::Error,
                _ => {
                    eprintln!("Invalid log level: {}, defaulting to INFO", level);
                    Level::Info
                }
            };

            max_level = std::cmp::max(max_level, level);
            PATHS.write().unwrap().insert(path.to_string(), level);
        }

        log::set_logger(&LOGGER).ok();
        log::set_max_level(max_level.to_level_filter());
    }
}

/// Test-only capture of console output, so tests can assert that lines actually
/// reached the console branch without trying to redirect real stdout. Not
/// compiled into the library when used as a dependency.
#[cfg(test)]
mod test_support {
    use std::sync::{LazyLock, Mutex};

    static CONSOLE: LazyLock<Mutex<Vec<String>>> = LazyLock::new(|| Mutex::new(Vec::new()));

    pub(super) fn record_console(line: String) {
        CONSOLE.lock().unwrap().push(line);
    }

    pub(super) fn clear_console() {
        CONSOLE.lock().unwrap().clear();
    }

    pub(super) fn captured_console() -> Vec<String> {
        CONSOLE.lock().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn it_works() {
        Logger::init("INFO");
        log::info!("Hello, world!");
    }

    #[test]
    fn parse_log_level_parses_known_levels_and_rejects_others() {
        assert_eq!(parse_log_level("warn"), Some(Level::Warn));
        assert_eq!(parse_log_level(" ERROR "), Some(Level::Error));
        assert_eq!(parse_log_level("Trace"), Some(Level::Trace));
        assert_eq!(parse_log_level("OFF"), None);
        assert_eq!(parse_log_level(""), None);
        assert_eq!(parse_log_level("bogus"), None);
    }

    #[test]
    #[serial]
    fn sink_receives_owned_snapshot() {
        use std::sync::Mutex;
        Logger::init("TRACE");
        clear_log_sink();
        test_support::clear_console();

        let captured: Arc<Mutex<Vec<LogSnapshot>>> = Arc::new(Mutex::new(Vec::new()));
        let sink_captured = Arc::clone(&captured);
        set_log_sink(Arc::new(move |snapshot| {
            sink_captured.lock().unwrap().push(snapshot);
        }));

        log::warn!("hello sink");
        clear_log_sink();

        let snapshots = captured.lock().unwrap();
        assert!(snapshots
            .iter()
            .any(|snap| snap.message == "hello sink" && snap.level == Level::Warn));
    }

    /// Constraint B: a log emitted *during* the capture hook still reaches the
    /// console, because the console `println!` is unconditional and runs before
    /// the (abstract) sink call.
    #[test]
    #[serial]
    fn console_prints_even_when_sink_reenters() {
        use std::sync::atomic::{AtomicBool, Ordering};
        Logger::init("TRACE");
        clear_log_sink();
        test_support::clear_console();

        // The sink simulates "a log happens during a capture write": the first
        // time it sees the outer message it logs again (once, to stay bounded).
        let reentered = Arc::new(AtomicBool::new(false));
        let guard = Arc::clone(&reentered);
        set_log_sink(Arc::new(move |snapshot: LogSnapshot| {
            if snapshot.message == "outer-message" && !guard.swap(true, Ordering::SeqCst) {
                log::error!("inner-during-capture");
            }
        }));

        log::warn!("outer-message");
        let console = test_support::captured_console();
        clear_log_sink();

        assert!(
            console.iter().any(|line| line.contains("outer-message")),
            "outer log must reach console"
        );
        assert!(
            console
                .iter()
                .any(|line| line.contains("inner-during-capture")),
            "a log emitted during a capture write must still reach console (Constraint B)"
        );
    }

    #[test]
    #[serial]
    fn test_log_levels() {
        Logger::init("DEBUG,path=TRACE,crate/module=ERROR");
        assert_eq!(
            LOGGER.enabled(
                &Metadata::builder()
                    .level(Level::Info)
                    .target("crate")
                    .build()
            ),
            true
        );
        assert_eq!(
            LOGGER.enabled(
                &Metadata::builder()
                    .level(Level::Debug)
                    .target("crate")
                    .build()
            ),
            true
        );
        assert_eq!(
            LOGGER.enabled(
                &Metadata::builder()
                    .level(Level::Trace)
                    .target("crate")
                    .build()
            ),
            false
        );
        assert_eq!(
            LOGGER.enabled(
                &Metadata::builder()
                    .level(Level::Error)
                    .target("crate/module")
                    .build()
            ),
            true
        );
        assert_eq!(
            LOGGER.enabled(
                &Metadata::builder()
                    .level(Level::Warn)
                    .target("crate/module")
                    .build()
            ),
            false
        );
        assert_eq!(
            LOGGER.enabled(
                &Metadata::builder()
                    .level(Level::Info)
                    .target("other")
                    .build()
            ),
            true
        );
        assert_eq!(
            LOGGER.enabled(
                &Metadata::builder()
                    .level(Level::Debug)
                    .target("other")
                    .build()
            ),
            true
        );
        assert_eq!(
            LOGGER.enabled(
                &Metadata::builder()
                    .level(Level::Trace)
                    .target("other")
                    .build()
            ),
            false
        );
    }

    #[test]
    #[serial]
    fn test_log_wrong_level() {
        Logger::init("WRONG");
        assert_eq!(
            LOGGER.enabled(
                &Metadata::builder()
                    .level(Level::Info)
                    .target("crate")
                    .build()
            ),
            true,
            "Default level is INFO"
        );
        assert_eq!(
            LOGGER.enabled(
                &Metadata::builder()
                    .level(Level::Debug)
                    .target("crate")
                    .build()
            ),
            false
        );
    }
}
