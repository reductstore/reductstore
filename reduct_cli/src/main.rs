// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod cmd;
mod config;
mod context;
mod helpers;
mod io;
mod parsers;

use crate::cmd::alias::{alias_cmd, alias_handler};
use crate::context::ContextBuilder;

use crate::cmd::bucket::{bucket_cmd, bucket_handler};
use crate::cmd::server::{server_cmd, server_handler};
use crate::cmd::token::{token_cmd, token_handler};
use clap::ArgAction::{SetFalse, SetTrue};
use clap::{crate_description, crate_name, crate_version, Arg, Command};

fn cli() -> Command {
    Command::new(crate_name!())
        .version(crate_version!())
        .arg_required_else_help(true)
        .about(crate_description!())
        .arg(
            Arg::new("ignore-ssl")
                .long("ignore-ssl")
                .short('i')
                .help("Ignore SSL certificate verification")
                .required(false)
                .action(SetTrue)
                .global(true),
        )
        .subcommand(alias_cmd())
        .subcommand(server_cmd())
        .subcommand(bucket_cmd())
        .subcommand(token_cmd())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let matches = cli().get_matches();
    let ctx = ContextBuilder::new()
        .ignore_ssl(matches.get_flag("ignore-ssl"))
        .build();

    match matches.subcommand() {
        Some(("alias", args)) => alias_handler(&ctx, args.subcommand()),
        Some(("server", args)) => server_handler(&ctx, args.subcommand()).await,
        Some(("bucket", args)) => bucket_handler(&ctx, args.subcommand()).await,
        Some(("token", args)) => token_handler(&ctx, args.subcommand()).await,
        _ => Ok(()),
    }?;

    Ok(())
}
