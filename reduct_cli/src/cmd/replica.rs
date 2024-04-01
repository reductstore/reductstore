// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod create;
mod ls;
mod rm;
mod show;
mod update;

use crate::cmd::replica::ls::{ls_replica, ls_replica_cmd};
use crate::cmd::replica::rm::{rm_replica_cmd, rm_replica_handler};
use crate::cmd::replica::show::{show_replica_cmd, show_replica_handler};
use crate::cmd::replica::update::{update_replica_cmd, update_replica_handler};
use clap::Command;
use create::{create_replica, create_replica_cmd};

pub(crate) fn replication_cmd() -> Command {
    Command::new("replica")
        .about("Manage replications in a ReductStore instance")
        .arg_required_else_help(true)
        .subcommand(create_replica_cmd())
        .subcommand(update_replica_cmd())
        .subcommand(ls_replica_cmd())
        .subcommand(show_replica_cmd())
        .subcommand(rm_replica_cmd())
}

pub(crate) async fn replication_handler(
    _ctx: &crate::context::CliContext,
    matches: Option<(&str, &clap::ArgMatches)>,
) -> anyhow::Result<()> {
    match matches {
        Some(("create", args)) => create_replica(_ctx, args).await?,
        Some(("update", args)) => update_replica_handler(_ctx, args).await?,
        Some(("ls", args)) => ls_replica(_ctx, args).await?,
        Some(("show", args)) => show_replica_handler(_ctx, args).await?,
        Some(("rm", args)) => rm_replica_handler(_ctx, args).await?,
        _ => (),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::context::CliContext;
    use crate::io::reduct::build_client;
    use reduct_rs::ReductClient;

    pub(crate) async fn prepare_replication(
        context: &CliContext,
        replica: &str,
        bucket: &str,
        bucket2: &str,
    ) -> anyhow::Result<ReductClient> {
        let client = build_client(&context, "local").await?;
        client.create_bucket(bucket).send().await?;
        client.create_bucket(bucket2).send().await?;

        client
            .create_replication(replica)
            .dst_host("http://localhost:8383")
            .src_bucket(bucket)
            .dst_bucket(bucket2)
            .send()
            .await?;

        Ok(client)
    }
}
