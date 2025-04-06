// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

#[derive(Debug, PartialEq, Clone)]
pub struct IoExtensionInfo {
    name: String,
    version: String,
}

pub struct IoExtensionInfoBuilder {
    name: String,
    version: String,
}

impl IoExtensionInfoBuilder {
    fn new() -> Self {
        Self {
            name: String::new(),
            version: String::new(),
        }
    }

    pub fn name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    pub fn version(mut self, version: &str) -> Self {
        self.version = version.to_string();
        self
    }

    pub fn build(self) -> IoExtensionInfo {
        IoExtensionInfo {
            name: self.name,
            version: self.version,
        }
    }
}

impl IoExtensionInfo {
    pub fn builder() -> IoExtensionInfoBuilder {
        IoExtensionInfoBuilder::new()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn version(&self) -> &str {
        &self.version
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_extension_info() {
        let info = IoExtensionInfo::builder()
            .name("TestExtension")
            .version("1.0.0")
            .build();

        assert_eq!(info.name(), "TestExtension");
        assert_eq!(info.version(), "1.0.0");
    }
}
