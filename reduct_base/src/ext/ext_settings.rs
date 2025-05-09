// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::msg::server_api::ServerInfo;

/// Settings for initializing an extension.
#[derive(Debug, PartialEq, Clone)]
pub struct ExtSettings {
    /// The log level for the extension.
    log_level: String,
    server_info: ServerInfo,
}

/// Builder for `ExtSettings`.
pub struct ExtSettingsBuilder {
    log_level: String,
    server_info: Option<ServerInfo>,
}

impl ExtSettingsBuilder {
    /// Creates a new `ExtSettingsBuilder`.
    fn new() -> Self {
        Self {
            log_level: "INFO".to_string(),
            server_info: None,
        }
    }
    /// Sets the log level for the extension.
    pub fn log_level(mut self, log_level: &str) -> Self {
        self.log_level = log_level.to_string();
        self
    }

    /// Sets the server info for the extension.
    pub fn server_info(mut self, server_info: ServerInfo) -> Self {
        self.server_info = Some(server_info);
        self
    }

    /// Builds the `ExtSettings`.
    ///
    /// # Panics
    ///
    /// Panics if the server info is not set.
    pub fn build(self) -> ExtSettings {
        ExtSettings {
            log_level: self.log_level,
            server_info: self.server_info.expect("Server info must be set"),
        }
    }
}

impl ExtSettings {
    /// Returns the log level for the extension.
    pub fn log_level(&self) -> &str {
        &self.log_level
    }

    /// Returns the server info for the extension.
    pub fn server_info(&self) -> &ServerInfo {
        &self.server_info
    }

    /// Creates a new `ExtSettingsBuilder`.
    pub fn builder() -> ExtSettingsBuilder {
        ExtSettingsBuilder::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ext_settings_builder() {
        let settings = ExtSettings::builder()
            .log_level("INFO")
            .server_info(ServerInfo {
                version: "1.0".to_string(),
                ..ServerInfo::default()
            })
            .build();

        assert_eq!(settings.log_level(), "INFO");
        assert_eq!(settings.server_info().version, "1.0");
    }
}
