// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::error::ReductError;
use crate::msg::server_api::Defaults;
use crate::Labels;
use bytes::Bytes;
use derive_builder::Builder;
use std::collections::BTreeMap;
use url::PathSegmentsMut;

#[derive(Debug, Clone)]
pub enum ConfigField {
    String {
        value: Option<String>,
        default: Option<String>,
        max_length: Option<usize>,
        min_length: Option<usize>,
        masked: bool,
    },
    Int {
        value: Option<i64>,
        default: Option<i64>,
        max: Option<i64>,
        min: Option<i64>,
    },
    Float {
        value: Option<f64>,
        default: Option<f64>,
        max: Option<f64>,
        min: Option<f64>,
    },
    Bool {
        value: Option<bool>,
        default: Option<bool>,
    },
}

#[derive(Builder, Debug, Clone)]
pub struct ConfigFiled {
    name: String,
    value: ConfigField,
}

pub type ExtensionConfig = BTreeMap<String, ConfigFiled>;

pub static API_VERSION: &str = "0";

#[derive(Debug, Builder, Clone)]
#[builder(setter(into))]
pub struct ExtensionInfo {
    name: String,
    version: String,
    rust_version: String,
    plugin_api_version: String,
    description: String,
    enabled: bool,
    pipeline_pos: i32, // Position in the pipeline with other plugins (0 = first)

    config: ExtensionConfig,
}

impl Default for ExtensionInfo {
    fn default() -> ExtensionInfo {
        Self {
            name: "".to_string(),
            version: "".to_string(),
            rust_version: env!("CARGO_PKG_RUST_VERSION").to_string(),
            plugin_api_version: API_VERSION.to_string(),
            description: "".to_string(),
            enabled: false,
            pipeline_pos: 0,
            config: BTreeMap::new(),
        }
    }
}

pub trait IoWriteExtension {
    fn start_write(
        &mut self,
        entry: &str,
        time: u64,
        data_type: &str,
        labels: Labels,
    ) -> Result<u64, ReductError>;

    fn write(&mut self, id: u64, data: Bytes) -> Result<Bytes, ReductError>;

    fn end_write(&mut self, id: u64) -> Result<(), ReductError>;
}

pub trait IoReadExtension {
    fn start_read(
        &mut self,
        entry: &str,
        time: u64,
        data_type: &str,
        labels: Labels,
    ) -> Result<u64, ReductError>;

    fn read(&mut self, id: u64, size: u64) -> Result<Bytes, ReductError>;

    fn end_read(&mut self, id: u64) -> Result<(), ReductError>;
}

pub trait IoExtension: IoWriteExtension + IoReadExtension {
    fn info(&self) -> &ExtensionInfo;

    fn enable(&mut self, yes: bool) -> Result<(), ReductError>;

    fn set_position(&mut self, pos: i32) -> Result<(), ReductError>;

    fn set_config(&mut self, key: &str, value: &str) -> Result<(), ReductError>;

    fn get_config(&self, key: &str) -> Result<String, ReductError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn test_plugin_info() {
        let info = ExtensionInfoBuilder::default()
            .name("test".to_string())
            .version("0.1.0".to_string())
            .description("A test plugin".to_string())
            .enabled(true)
            .pipeline_pos(0)
            .config(BTreeMap::new())
            .build()
            .unwrap();

        assert_eq!(info.name, "test");
        assert_eq!(info.version, "0.1.0");
        assert_eq!(info.description, "A test plugin");
        assert_eq!(info.enabled, true);
        assert_eq!(info.plugin_api_version, API_VERSION.to_string());
        assert_eq!(info.rust_version, env!("CARGO_PKG_RUST_VERSION"));
        assert_eq!(info.pipeline_pos, 0);
        assert_eq!(info.config.len(), 0);
    }
}
