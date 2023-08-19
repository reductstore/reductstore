// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::{Alias, Config};
use anyhow::{Context, Error};
use clap::{arg, Command};
use url::Url;

pub(super) fn add_alias(name: &str, url: &str, token: &str) -> anyhow::Result<()> {
    let mut config = Config::load()?;
    if config.aliases.contains_key(name) {
        return Err(Error::msg(format!("Alias '{}' already exists", url)));
    }

    config.aliases.insert(
        name.to_string(),
        Alias {
            url: Url::parse(url)?,
            token: token.to_string(),
        },
    );
    config.save()?;
    Ok(())
}

pub(super) fn add_alias_cmd() -> Command {
    Command::new("add")
        .about("Add an alias")
        .arg(arg!(<NAME> "The name of the alias to create").required(true))
        .arg(arg!(<URL> "The URL of the ReductStore instance").required(true))
        .arg(arg!(<TOKEN> "The token to use for authentication").required(false))
}
