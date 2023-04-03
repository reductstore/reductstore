// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use log::{Record, Level, Metadata, Log, info};
use chrono::prelude::{DateTime, Utc};
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
            //  std::cout << fmt::format(fmt::fg(color), "{}.{:03d} ({:>5}) {:>7} -- {}:{} {}", ss.str(), milliseconds,
            //                                reinterpret_cast<uint16_t &>(thid), level_str, file, line, msg)
            //                 << std::endl;

            let now: DateTime<Utc> = Utc::now();
            println!("{} ({:>5}) [{}] -- {:}:{:} {:?}", now.format("%Y-%m-%d %H:%M:%S.%3f"),
                     thread_id::get() % 100000, record.level(), record.file().unwrap(), record.line().unwrap(), record.args());
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
        log::set_logger(&LOGGER).unwrap();
        match level {
            "TRACE" => log::set_max_level(Level::Trace.to_level_filter()),
            "DEBUG" => log::set_max_level(Level::Debug.to_level_filter()),
            "INFO" => log::set_max_level(Level::Info.to_level_filter()),
            "WARN" => log::set_max_level(Level::Warn.to_level_filter()),
            "ERROR" => log::set_max_level(Level::Error.to_level_filter()),
            _ =>
                {
                    log::set_max_level(Level::Info.to_level_filter());
                    info!("Invalid log level: {}, defaulting to INFO", level);
                }
        }
    }
}

/// Initialize the logger (C++ wrapper).
pub fn init_log(level: &str) {
    Logger::init(level);
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
