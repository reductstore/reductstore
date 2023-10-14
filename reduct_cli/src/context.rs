// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dirs::home_dir;
use std::env::current_dir;

pub(crate) trait Output {
    fn print(&self, message: &str);

    fn history(&self) -> Vec<String>;
}

struct StdoutOutput;

impl Output for StdoutOutput {
    fn print(&self, message: &str) {
        println!("{}", message);
    }

    fn history(&self) -> Vec<String> {
        Vec::new()
    }
}

pub(crate) struct CliContext {
    config_path: String,
    output: Box<dyn Output>,
}

impl CliContext {
    pub(crate) fn config_path(&self) -> &str {
        &self.config_path
    }
    pub(crate) fn output(&self) -> &dyn Output {
        &*self.output
    }
}

pub(crate) struct ContextBuilder {
    config: CliContext,
}

impl ContextBuilder {
    pub(crate) fn new() -> Self {
        let mut config = CliContext {
            config_path: String::new(),
            output: Box::new(StdoutOutput {}),
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

    pub(crate) fn output(mut self, output: Box<dyn Output>) -> Self {
        self.config.output = output;
        self
    }

    pub(crate) fn build(self) -> CliContext {
        self.config
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::config::{Alias, ConfigFile};
    use crate::reduct::build_client;
    use rstest::fixture;
    use std::cell::RefCell;
    use std::task::Context;
    use tempfile::tempdir;

    pub struct MockOutput {
        history: RefCell<Vec<String>>,
    }

    impl Output for MockOutput {
        fn print(&self, message: &str) {
            self.history.borrow_mut().push(message.to_string());
        }

        fn history(&self) -> Vec<String> {
            self.history.borrow().clone()
        }
    }

    impl MockOutput {
        pub fn new() -> Self {
            MockOutput {
                history: RefCell::new(Vec::new()),
            }
        }
    }

    #[fixture]
    pub(crate) fn output() -> Box<MockOutput> {
        Box::new(MockOutput::new())
    }

    #[fixture]
    pub(crate) fn current_token() -> String {
        std::env::var("RS_API_TOKEN").unwrap_or_default()
    }

    #[fixture]
    pub(crate) fn context(output: Box<dyn Output>, current_token: String) -> CliContext {
        let tmp_dir = tempdir().unwrap();
        let ctx = ContextBuilder::new()
            .config_path(tmp_dir.into_path().join("config.toml").to_str().unwrap())
            .output(output)
            .build();

        // add a default alias
        let mut config_file = ConfigFile::load(ctx.config_path()).unwrap();
        let config = config_file.mut_config();
        config.aliases.insert(
            "default".to_string(),
            Alias {
                url: url::Url::parse("https://default.store").unwrap(),
                token: "test_token".to_string(),
            },
        );
        config.aliases.insert(
            "local".to_string(),
            Alias {
                url: url::Url::parse("http://localhost:8383").unwrap(),
                token: current_token,
            },
        );
        config_file.save().unwrap();
        ctx
    }

    #[fixture]
    pub(crate) async fn bucket(context: CliContext) -> String {
        let client = build_client(&context, "local").unwrap();
        // remove the bucket if it already exists
        if let Ok(bucket) = client.get_bucket("test_bucket").await {
            bucket.remove().await.unwrap_or_default();
        }

        "test_bucket".to_string()
    }
}
