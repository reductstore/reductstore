// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// Settings for initializing an extension.
#[derive(Debug, PartialEq, Clone)]
pub struct ExtSettings {
    /// The log level for the extension.
    log_level: String,
}

impl Default for ExtSettings {
    fn default() -> Self {
        Self {
            log_level: "INFO".to_string(),
        }
    }
}

/// Builder for `ExtSettings`.
pub struct ExtSettingsBuilder {
    settings: ExtSettings,
}

impl ExtSettingsBuilder {
    /// Creates a new `ExtSettingsBuilder`.
    fn new() -> Self {
        Self {
            settings: ExtSettings::default(),
        }
    }
    /// Sets the log level for the extension.
    pub fn log_level(mut self, log_level: &str) -> Self {
        self.settings.log_level = log_level.to_string();
        self
    }

    pub fn build(self) -> ExtSettings {
        self.settings
    }
}

impl ExtSettings {
    /// Returns the log level for the extension.
    pub fn log_level(&self) -> &str {
        &self.log_level
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
        let settings = ExtSettings::builder().log_level("info").build();

        assert_eq!(settings.log_level(), "info");
    }
}
