// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
mod create;

use crate::context::CliContext;
use clap::Command;

pub(crate) fn bucket_cmd() -> Command {
    Command::new("bucket")
        .about("Manage buckets")
        .arg_required_else_help(true)
        .subcommand(create::create_bucket_cmd())
}

pub(crate) async fn bucket_handler(
    ctx: &CliContext,
    matches: Option<(&str, &clap::ArgMatches)>,
) -> anyhow::Result<()> {
    match matches {
        Some(("create", args)) => create::crate_bucket(ctx, args)?,
        _ => (),
    }

    Ok(())
}
