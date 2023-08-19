use clap::Command;

// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
mod add;

pub(crate) fn alias_cmd() -> Command {
    Command::new("alias")
        .about("Manage aliases for different ReductStore instances")
        .subcommand(add::add_alias_cmd())
}

pub(crate) fn alias_handler(matches: Option<(&str, &clap::ArgMatches)>) -> anyhow::Result<()> {
    if let Some(("alias", matches)) = matches {
        match matches.subcommand() {
            Some(("add", args)) => add::add_alias(
                args.get_one::<String>("NAME").unwrap(),
                args.get_one::<String>("URL").unwrap(),
                args.get_one::<String>("TOKEN").unwrap_or(&String::new()),
            )?,
            _ => {}
        }
    }

    Ok(())
}
