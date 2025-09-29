// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::prelude::{DateTime, Utc};
use log::{info, Level, Log, Metadata, Record};
use std::collections::BTreeMap;
use std::sync::{LazyLock, RwLock};
use thread_id;

static LOGGER: Logger = Logger;

pub struct Logger;
static PATHS: LazyLock<RwLock<BTreeMap<String, Level>>> =
    LazyLock::new(|| RwLock::new(BTreeMap::new()));
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

            println!(
                "{} ({:>5}) [{}] -- {:}/{:}:{:} {:?}",
                now.format("%Y-%m-%d %H:%M:%S.%3f"),
                thread_id::get() % 100000,
                record.level(),
                package_name,
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
    pub fn init(levels: &str) {
        let mut max_level = Level::Error;
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
                    log::set_max_level(Level::Info.to_level_filter());
                    info!("Invalid log level: {}, defaulting to INFO", level);
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
