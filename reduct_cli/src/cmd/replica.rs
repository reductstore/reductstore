// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod create;
mod ls;

use crate::cmd::replica::ls::{ls_replica, ls_replica_cmd};
use clap::Command;
use create::{create_replica, create_replica_cmd};

pub(crate) fn replication_cmd() -> Command {
    Command::new("replica")
        .about("Manage replications in a ReductStore instance")
        .arg_required_else_help(true)
        .subcommand(create_replica_cmd())
        .subcommand(ls_replica_cmd())
}

pub(crate) async fn replication_handler(
    _ctx: &crate::context::CliContext,
    matches: Option<(&str, &clap::ArgMatches)>,
) -> anyhow::Result<()> {
    match matches {
        Some(("create", args)) => create_replica(_ctx, args).await?,
        Some(("ls", args)) => ls_replica(_ctx, args).await?,
        _ => (),
    }
    Ok(())
}
