// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod create;
mod rm;

use crate::cmd::token::create::create_token_cmd;
use crate::cmd::token::rm::rm_token_cmd;
use crate::context::CliContext;
use clap::Command;

pub(crate) fn token_cmd() -> Command {
    Command::new("token")
        .about("Manage access tokens in a ReductStore instance")
        .arg_required_else_help(true)
        .subcommand(create_token_cmd())
        .subcommand(rm_token_cmd())
}

pub(crate) async fn token_handler(
    ctx: &CliContext,
    matches: Option<(&str, &clap::ArgMatches)>,
) -> anyhow::Result<()> {
    match matches {
        Some(("create", args)) => create::create_token(ctx, args).await?,
        Some(("rm", args)) => rm::rm_token(ctx, args).await?,
        _ => (),
    }
    Ok(())
}
