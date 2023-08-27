// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::context::Context;
use clap::Command;

mod add;

pub(crate) fn alias_cmd() -> Command {
    Command::new("alias")
        .about("Manage aliases for different ReductStore instances")
        .subcommand(add::add_alias_cmd())
}

pub(crate) fn alias_handler(
    ctx: &Context,
    matches: Option<(&str, &clap::ArgMatches)>,
) -> anyhow::Result<()> {
    match matches {
        Some(("add", args)) => add::add_alias(
            ctx,
            args.get_one::<String>("NAME").unwrap(),
            args.get_one::<String>("URL").unwrap(),
            args.get_one::<String>("TOKEN").unwrap_or(&String::new()),
        )?,
        _ => {}
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::context;
    use rstest::rstest;

    #[rstest]
    fn test_alias_handler_add(context: Context) {
        let matches =
            alias_cmd().get_matches_from(vec!["alias", "add", "test", "XXX", "test_token"]);
        assert_eq!(
            alias_handler(&context, matches.subcommand())
                .err()
                .unwrap()
                .to_string(),
            "relative URL without a base",
            "Check handling of alias add command"
        );
    }
}
