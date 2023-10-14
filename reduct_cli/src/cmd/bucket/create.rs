// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::parsers::{ByteSizeParser, QuotaTypeParser};
use crate::cmd::BUCKET_PATH_HELP;
use crate::context::CliContext;
use crate::reduct::build_client;
use bytesize::ByteSize;
use clap::builder::RangedU64ValueParser;
use clap::{Arg, ArgMatches, Command};
use reduct_rs::{BucketSettings, QuotaType, ReductClient};

pub(super) fn create_bucket_cmd() -> Command {
    Command::new("create")
        .about("create a bucket")
        .arg(
            Arg::new("BUCKET_PATH")
                .help(BUCKET_PATH_HELP)
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

pub(super) async fn create_bucket(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let path = args.get_one::<String>("BUCKET_PATH").unwrap();
    if let Some((alias_or_url, bucket_name)) = path.rsplit_once('/') {
        let bucket_settings = BucketSettings {
            quota_type: args.get_one::<QuotaType>("quota-type").map(|v| v.clone()),
            quota_size: args.get_one::<ByteSize>("quota-size").map(|v| v.as_u64()),
            max_block_size: args.get_one::<ByteSize>("block-size").map(|v| v.as_u64()),
            max_block_records: args.get_one::<u64>("block-records").map(|v| v.clone()),
        };

        let client: ReductClient = build_client(ctx, alias_or_url)?;
        client
            .create_bucket(bucket_name)
            .settings(bucket_settings)
            .send()
            .await?;

        println!("Bucket '{}' created", bucket_name);
        Ok(())
    } else {
        Err(anyhow::anyhow!("Invalid bucket path"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::context;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket(context: CliContext) {
        let args = create_bucket_cmd().get_matches_from(vec![
            "create",
            "local/test",
            "--quota-type",
            "FIFO",
            "--quota-size",
            "1GB",
            "--block-size",
            "64MB",
            "--block-records",
            "1024",
        ]);

        assert_eq!(
            create_bucket(&context, &args).await.unwrap(),
            (),
            "Create bucket succeeded"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket_invalid_path(context: CliContext) {
        let args = create_bucket_cmd().get_matches_from(vec!["create", "local"]);

        assert!(create_bucket(&context, &args).await.is_err());
    }
}
