// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::context::CliContext;
use clap::Command;

mod add;
mod ls;
mod rm;

pub(crate) fn alias_cmd() -> Command {
    Command::new("alias")
        .about("Manage aliases for different ReductStore instances")
        .arg_required_else_help(true)
        .subcommand(add::add_alias_cmd())
        .subcommand(ls::ls_aliases_cmd())
        .subcommand(rm::rm_alias_cmd())
}

pub(crate) fn alias_handler(
    ctx: &CliContext,
    matches: Option<(&str, &clap::ArgMatches)>,
) -> anyhow::Result<()> {
    match matches {
        Some(("add", args)) => add::add_alias(ctx, args)?,
        Some(("ls", _)) => ls::list_aliases(ctx)?,
        Some(("rm", args)) => rm::remove_alias(ctx, args)?,
        _ => (),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::context;
    use rstest::rstest;

    #[rstest]
    fn test_alias_handler_add(context: CliContext) {
        let matches = alias_cmd().get_matches_from(vec!["alias", "add", "test", "-L", "XXX"]);
        assert_eq!(
            alias_handler(&context, matches.subcommand())
                .err()
                .unwrap()
                .to_string(),
            "relative URL without a base",
            "Check handling of alias add command"
        );
    }

    #[rstest]
    fn test_alias_handler_ls(context: CliContext) {
        let matches = alias_cmd().get_matches_from(vec!["alias", "ls"]);
        assert_eq!(
            alias_handler(&context, matches.subcommand()).unwrap(),
            (),
            "Check handling of alias ls command"
        );
    }

    #[rstest]
    fn test_alias_handler_rm(context: CliContext) {
        let matches = alias_cmd().get_matches_from(vec!["alias", "rm", "test"]);
        assert_eq!(
            alias_handler(&context, matches.subcommand())
                .err()
                .unwrap()
                .to_string(),
            "Alias 'test' does not exist",
            "Check handling of alias rm command"
        );
    }
}
