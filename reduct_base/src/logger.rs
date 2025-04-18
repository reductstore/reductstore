// Copyright 2023 ReductSoftware UG
// Licensed under the Business Source License 1.1

use chrono::prelude::{DateTime, Utc};
use log::{info, Level, Log, Metadata, Record};
use thread_id;

static LOGGER: Logger = Logger;

/// Initialize the logger.
pub struct Logger;

impl Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &Record) {
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

            println!(
                "{} ({:>5}) [{}] -- ({:}) {:}:{:} {:?}",
                now.format("%Y-%m-%d %H:%M:%S.%3f"),
                thread_id::get() % 100000,
                record.level(),
                record.target(),
                file,
                record.line().unwrap(),
                record.args(),
            );
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
    pub fn init(level: &str) {
        log::set_logger(&LOGGER).ok();
        match level {
            "TRACE" => log::set_max_level(Level::Trace.to_level_filter()),
            "DEBUG" => log::set_max_level(Level::Debug.to_level_filter()),
            "INFO" => log::set_max_level(Level::Info.to_level_filter()),
            "WARN" => log::set_max_level(Level::Warn.to_level_filter()),
            "ERROR" => log::set_max_level(Level::Error.to_level_filter()),
            _ => {
                log::set_max_level(Level::Info.to_level_filter());
                info!("Invalid log level: {}, defaulting to INFO", level);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        Logger::init("INFO");
        log::info!("Hello, world!");
    }
}
