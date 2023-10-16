// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::ConfigFile;
use crate::context::CliContext;
use anyhow::Error;
use clap::{arg, ArgMatches, Command};

pub(super) fn remove_alias(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let name = args.get_one::<String>("NAME").unwrap();

    let mut config_file = ConfigFile::load(ctx.config_path())?;
    let config = config_file.mut_config();
    if !config.aliases.contains_key(name) {
        return Err(Error::msg(format!("Alias '{}' does not exist", name)));
    }

    config.aliases.remove(name);
    config_file.save()?;
    Ok(())
}

pub(super) fn rm_alias_cmd() -> Command {
    Command::new("rm")
        .about("Remove an alias")
        .arg(arg!(<NAME> "The name of the alias to remove").required(true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::context;
    use rstest::rstest;

    #[rstest]
    fn test_remove_alias(context: CliContext) {
        let args = rm_alias_cmd().get_matches_from(vec!["rm", "default"]);
        remove_alias(&context, &args).unwrap();

        let config_file = ConfigFile::load(context.config_path()).unwrap();
        let config = config_file.config();
        assert!(!config.aliases.contains_key("default"));
    }

    #[rstest]
    fn test_remove_alias_not_found(context: CliContext) {
        let args = rm_alias_cmd().get_matches_from(vec!["rm", "test"]);
        let result = remove_alias(&context, &args);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Alias 'test' does not exist"
        );
    }
}
