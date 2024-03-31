// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::RESOURCE_PATH_HELP;
use crate::io::reduct::build_client;
use crate::io::std::output;
use clap::{Arg, Command};
use tabled::settings::Style;
use tabled::{Table, Tabled};

pub(super) fn show_replica_cmd() -> Command {
    Command::new("show").about("Show replica details").arg(
        Arg::new("REPLICATION_PATH")
            .help(RESOURCE_PATH_HELP)
            .value_parser(crate::parse::ResourcePathParser::new())
            .required(true),
    )
}

#[derive(Tabled)]
struct ErrorRow {
    #[tabled(rename = "Error Code")]
    code: i16,
    #[tabled(rename = "Count")]
    count: u64,
    #[tabled(rename = "Last Message")]
    message: String,
}

pub(super) async fn show_replica_handler(
    ctx: &crate::context::CliContext,
    args: &clap::ArgMatches,
) -> anyhow::Result<()> {
    let (alias_or_url, replication_name) = args
        .get_one::<(String, String)>("REPLICATION_PATH")
        .unwrap();
    let client = build_client(ctx, alias_or_url).await?;

    let replica = client.get_replication(replication_name).await?;
    output!(
        ctx,
        "Name:                {:20} Source Bucket:         {}",
        replica.info.name,
        replica.settings.src_bucket
    );
    output!(
        ctx,
        "Active:              {:20} Destination Bucket:    {}",
        replica.info.is_active,
        replica.settings.dst_bucket
    );

    output!(
        ctx,
        "Provisioned:         {:20} Destination Server:    {}",
        replica.info.is_provisioned,
        replica.settings.dst_host
    );
    output!(
        ctx,
        "Pending Records:     {:<20} Entries:               {:?}",
        replica.info.pending_records,
        replica.settings.entries
    );
    output!(
        ctx,
        "Ok Records (hourly): {:<20} Include:               {:?}",
        replica.diagnostics.hourly.ok,
        replica.settings.include
    );
    output!(
        ctx,
        "Errors (hourly):     {:<20} Exclude:               {:?}\n",
        replica.diagnostics.hourly.errored,
        replica.settings.exclude
    );

    let table = Table::new(
        replica
            .diagnostics
            .hourly
            .errors
            .into_iter()
            .map(|(code, err)| ErrorRow {
                code,
                count: err.count,
                message: err.last_message,
            }),
    )
    .with(Style::markdown())
    .to_string();

    output!(ctx, "{}", table);
    Ok(())
}
