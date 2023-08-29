// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod cmd;
mod config;
mod context;

use crate::cmd::alias::{alias_cmd, alias_handler};
use crate::context::ContextBuilder;

use crate::cmd::server::{server_cmd, server_handler};
use clap::{crate_description, crate_name, crate_version, Command};

fn cli() -> Command {
    Command::new(crate_name!())
        .version(crate_version!())
        .arg_required_else_help(true)
        .about(crate_description!())
        .subcommand(alias_cmd())
        .subcommand(server_cmd())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ctx = ContextBuilder::new().build();
    let matches = cli().get_matches();
    match matches.subcommand() {
        Some(("alias", args)) => alias_handler(&ctx, args.subcommand()),
        Some(("server", args)) => server_handler(&ctx, args.subcommand()).await,
        _ => Ok(()),
    }?;

    Ok(())
}
