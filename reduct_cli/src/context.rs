// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dirs::home_dir;
use std::env::current_dir;

#[derive(Debug)]
pub(crate) struct Context {
    config_path: String,
}

impl Context {
    pub(crate) fn config_path(&self) -> &str {
        &self.config_path
    }
}

pub(crate) struct ContextBuilder {
    config: Context,
}

impl ContextBuilder {
    pub(crate) fn new() -> Self {
        let mut config = Context {
            config_path: String::new(),
        };
        config.config_path = match home_dir() {
            Some(path) => path
                .join(".reduct-cli/config.toml")
                .to_str()
                .unwrap()
                .to_string(),
            None => current_dir()
                .unwrap()
                .join(".reduct-cli/config.toml")
                .to_str()
                .unwrap()
                .to_string(),
        };
        ContextBuilder { config }
    }

    pub(crate) fn config_path(mut self, config_dir: &str) -> Self {
        self.config.config_path = config_dir.to_string();
        self
    }

    pub(crate) fn build(self) -> Context {
        self.config
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use rstest::fixture;
    use tempfile::tempdir;

    #[fixture]
    pub(crate) fn context() -> Context {
        let tmp_dir = tempdir().unwrap();
        ContextBuilder::new()
            .config_path(tmp_dir.into_path().join("config.toml").to_str().unwrap())
            .build()
    }
}
