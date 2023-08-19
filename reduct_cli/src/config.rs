use anyhow::Context;
use std::collections::HashMap;
// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::CONTEXT;
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Deserialize, Serialize)]
pub(crate) struct Alias {
    pub url: Url,
    pub token: String,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct Config {
    pub aliases: HashMap<String, Alias>,
}

impl Config {
    pub(crate) fn load() -> anyhow::Result<Config> {
        let path = CONTEXT.get().unwrap().config_path();
        match std::fs::read_to_string(path) {
            Ok(config) => Ok(toml::from_str(&config)
                .with_context(|| format!("Failed to parse config file {}", path))?),
            Err(_) => Ok(Config {
                aliases: HashMap::new(),
            }),
        }
    }

    pub(crate) fn save(&self) -> anyhow::Result<()> {
        let config = toml::to_string(&self)?;
        let path = CONTEXT.get().unwrap().config_path();

        if let Some(parent) = std::path::Path::new(path).parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create config directory {}", parent.display())
            })?;
        }

        std::fs::write(path, config)
            .with_context(|| format!("Failed to write config file {}", path))?;
        Ok(())
    }
}
