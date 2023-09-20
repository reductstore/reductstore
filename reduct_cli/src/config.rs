// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::context::CliContext;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use std::path::PathBuf;
use url::Url;

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub(crate) struct Alias {
    pub url: Url,
    pub token: String,
}

#[derive(Deserialize, Serialize, PartialEq, Debug)]
pub(crate) struct Config {
    pub aliases: BTreeMap<String, Alias>,
}

pub(crate) struct ConfigFile {
    path: PathBuf,
    config: Config,
}

impl ConfigFile {
    pub fn load(path: &str) -> anyhow::Result<ConfigFile> {
        let config: anyhow::Result<Config> = match std::fs::read_to_string(path) {
            Ok(config) => Ok(toml::from_str(&config)
                .with_context(|| format!("Failed to parse config file {:?}", path))?),
            Err(_) => Ok(Config {
                aliases: BTreeMap::new(),
            }),
        };

        Ok(ConfigFile {
            path: PathBuf::from(path),
            config: config?,
        })
    }

    pub fn save(&self) -> anyhow::Result<()> {
        let config = toml::to_string(&self.config)?;

        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create config directory {}", parent.display())
            })?;
        }

        std::fs::write(&self.path, config)
            .with_context(|| format!("Failed to write config file {:?}", &self.path,))?;
        Ok(())
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn mut_config(&mut self) -> &mut Config {
        &mut self.config
    }
}

pub(crate) fn find_alias(ctx: &CliContext, alias: &str) -> anyhow::Result<Alias> {
    let config_file = ConfigFile::load(ctx.config_path())?;
    let config = config_file.config();
    match config.aliases.get(alias) {
        Some(alias) => Ok((*alias).clone()),
        None => Err(anyhow::Error::msg(format!(
            "Alias '{}' does not exist",
            alias
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use std::fs;
    use std::fs::File;
    use std::io::Write;

    use crate::context::tests::context;
    use crate::context::CliContext;

    #[rstest]
    fn test_load(context: CliContext) {
        let mut file = File::create(context.config_path()).unwrap();
        file.write_all(
            r#"
            [aliases]
            test = { url = "https://test.com", token = "test" }
            "#
            .as_bytes(),
        )
        .unwrap();

        let config_file = ConfigFile::load(context.config_path()).unwrap();
        let config = config_file.config();
        assert_eq!(config.aliases.len(), 1);
        assert_eq!(
            config.aliases.get("test").unwrap().url.as_str(),
            "https://test.com/"
        );
        assert_eq!(config.aliases.get("test").unwrap().token, "test");
    }

    #[rstest]
    fn test_save(context: CliContext) {
        let mut config_file = ConfigFile::load(context.config_path()).unwrap();
        let config = config_file.mut_config();
        config.aliases = vec![(
            "test".to_string(),
            Alias {
                url: Url::parse("https://test.com").unwrap(),
                token: "test".to_string(),
            },
        )]
        .into_iter()
        .collect();

        config_file.save().unwrap();
        let cfg: Config =
            toml::from_str(&fs::read_to_string(context.config_path()).unwrap()).unwrap();
        assert_eq!(
            cfg.aliases.get("test").unwrap().url.as_str(),
            "https://test.com/"
        );
    }

    #[rstest]
    fn test_empty_config(context: CliContext) {
        let config_file = ConfigFile::load(&format!("{}.empty", context.config_path())).unwrap();
        assert_eq!(config_file.config().aliases.len(), 0);
    }
}
