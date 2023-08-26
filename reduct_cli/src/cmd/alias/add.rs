// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::{Alias, ConfigFile};
use anyhow::Error;
use clap::{arg, Command};
use url::Url;

use crate::context::Context;

pub(super) fn add_alias(ctx: &Context, name: &str, url: &str, token: &str) -> anyhow::Result<()> {
    let mut config_file = ConfigFile::load(ctx.config_path())?;
    let config = config_file.mut_config();
    if config.aliases.contains_key(name) {
        return Err(Error::msg(format!("Alias '{}' already exists", name)));
    }

    config.aliases.insert(
        name.to_string(),
        Alias {
            url: Url::parse(url)?,
            token: token.to_string(),
        },
    );
    config_file.save()?;
    Ok(())
}

pub(super) fn add_alias_cmd() -> Command {
    Command::new("add")
        .about("Add an alias")
        .arg(arg!(<NAME> "The name of the alias to create").required(true))
        .arg(arg!(<URL> "The URL of the ReductStore instance").required(true))
        .arg(arg!(<TOKEN> "The token to use for authentication").required(false))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::context::tests::context;

    use rstest::rstest;

    #[rstest]
    fn test_add_alias(context: Context) {
        add_alias(&context, "test", "https://test.reduct.store", "test_token").unwrap();

        let config_file = ConfigFile::load(context.config_path()).unwrap();
        let config = config_file.config();
        assert_eq!(config.aliases.len(), 1);
        assert!(config.aliases.contains_key("test"));
        assert_eq!(
            *config.aliases.get("test").unwrap(),
            Alias {
                url: Url::parse("https://test.reduct.store").unwrap(),
                token: "test_token".to_string(),
            }
        );
    }

    #[rstest]
    fn test_add_bad_url(context: Context) {
        let result = add_alias(&context, "test", "bad_url", "test_token");
        assert_eq!(
            result.err().unwrap().to_string(),
            "relative URL without a base"
        );
    }

    #[rstest]
    fn test_add_existing_alias(context: Context) {
        add_alias(&context, "test", "https://test.reduct.store", "test_token").unwrap();

        let result = add_alias(&context, "test", "https://test.reduct.store", "test_token");
        assert_eq!(
            result.err().unwrap().to_string(),
            "Alias 'test' already exists"
        );
    }
}
