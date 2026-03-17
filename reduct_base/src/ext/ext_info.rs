// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

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
