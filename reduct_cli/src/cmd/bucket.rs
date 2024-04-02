// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
mod create;
mod ls;
mod rm;
mod show;
mod update;

use crate::cmd::RESOURCE_PATH_HELP;
use crate::context::CliContext;
use crate::parse::{ByteSizeParser, QuotaTypeParser, ResourcePathParser};
use bytesize::ByteSize;
use clap::builder::RangedU64ValueParser;
use clap::{Arg, ArgMatches, Command};
use reduct_rs::{BucketSettings, QuotaType};

pub(crate) fn bucket_cmd() -> Command {
    Command::new("bucket")
        .about("Manage buckets in a ReductStore instance")
        .arg_required_else_help(true)
        .subcommand(create::create_bucket_cmd())
        .subcommand(update::update_bucket_cmd())
        .subcommand(rm::rm_bucket_cmd())
        .subcommand(ls::ls_bucket_cmd())
        .subcommand(show::show_bucket_cmd())
}

pub(crate) async fn bucket_handler(
    ctx: &CliContext,
    matches: Option<(&str, &ArgMatches)>,
) -> anyhow::Result<()> {
    match matches {
        Some(("create", args)) => create::create_bucket(ctx, args).await?,
        Some(("update", args)) => update::update_bucket(ctx, args).await?,
        Some(("rm", args)) => rm::rm_bucket(ctx, args).await?,
        Some(("ls", args)) => ls::ls_bucket(ctx, args).await?,
        Some(("show", args)) => show::show_bucket(ctx, args).await?,
        _ => (),
    }

    Ok(())
}

/// Create or update bucket arguments to avoid duplication
fn create_update_bucket_args(cmd: Command) -> Command {
    cmd.arg(
        Arg::new("BUCKET_PATH")
            .help(RESOURCE_PATH_HELP)
            .value_parser(ResourcePathParser::new())
            .required(true),
    )
    .arg(
        Arg::new("quota-type")
            .long("quota-type")
            .short('Q')
            .value_name("TEXT")
            .value_parser(QuotaTypeParser::new())
            .help("Quota type. Must be NONE or FIFO")
            .required(false),
    )
    .arg(
        Arg::new("quota-size")
            .long("quota-size")
            .short('s')
            .value_name("SIZE")
            .value_parser(ByteSizeParser::new())
            .help("Quota size in CI format e.g. 1Mb or 3TB"),
    )
    .arg(
        Arg::new("block-size")
            .long("block-size")
            .short('b')
            .value_name("SIZE")
            .value_parser(ByteSizeParser::new())
            .help("Max. bock size in CI format e.g 64MB"),
    )
    .arg(
        Arg::new("block-records")
            .long("block-records")
            .short('R')
            .value_name("NUMBER")
            .value_parser(RangedU64ValueParser::<u64>::new().range(32..4048))
            .help("Max. number of records in a block"),
    )
}

fn parse_bucket_settings(args: &ArgMatches) -> BucketSettings {
    let bucket_settings = BucketSettings {
        quota_type: args.get_one::<QuotaType>("quota-type").map(|v| v.clone()),
        quota_size: args.get_one::<ByteSize>("quota-size").map(|v| v.as_u64()),
        max_block_size: args.get_one::<ByteSize>("block-size").map(|v| v.as_u64()),
        max_block_records: args.get_one::<u64>("block-records").map(|v| v.clone()),
    };
    bucket_settings
}
