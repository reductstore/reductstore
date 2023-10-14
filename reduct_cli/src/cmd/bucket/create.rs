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
    use crate::context::tests::{bucket, context};
    use rstest::*;

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket(context: CliContext, #[future] bucket: String) {
        let args = create_bucket_cmd().get_matches_from(vec![
            "create",
            format!("local/{}", bucket.await).as_str(),
            "--quota-type",
            "FIFO",
            "--quota-size",
            "1GB",
            "--block-size",
            "32MB",
            "--block-records",
            "100",
        ]);

        assert_eq!(
            create_bucket(&context, &args).await.unwrap(),
            (),
            "Create bucket succeeded"
        );

        let client = build_client(&context, "local").unwrap();
        let bucket = client.get_bucket("test_bucket").await.unwrap();
        let settings = bucket.settings().await.unwrap();
        assert_eq!(settings.quota_type, Some(QuotaType::FIFO));
        assert_eq!(settings.quota_size, Some(1_000_000_000));
        assert_eq!(settings.max_block_size, Some(32_000_000));
        assert_eq!(settings.max_block_records, Some(100));
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket_invalid_path(context: CliContext) {
        let args = create_bucket_cmd().get_matches_from(vec!["create", "local"]);
        assert_eq!(
            create_bucket(&context, &args)
                .await
                .err()
                .unwrap()
                .to_string(),
            "Invalid bucket path",
            "Failed because of invalid path"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket_invalid_quota_type(context: CliContext, #[future] bucket: String) {
        let args = create_bucket_cmd().try_get_matches_from(vec![
            "create",
            format!("local/{}", bucket.await).as_str(),
            "--quota-type",
            "INVALID",
        ]);
        assert_eq!(
            args.err().unwrap().to_string(),
            "error: invalid value 'INVALID' for '--quota-type <TEXT>'\n\nFor more information, try '--help'.\n",
            "Failed because of invalid quota type"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket_invalid_quota_size(context: CliContext, #[future] bucket: String) {
        let args = create_bucket_cmd().try_get_matches_from(vec![
            "create",
            format!("local/{}", bucket.await).as_str(),
            "--quota-size",
            "INVALID",
        ]);
        assert_eq!(
            args.err().unwrap().to_string(),
            "error: invalid value 'INVALID' for '--quota-size <SIZE>'\n\nFor more information, try '--help'.\n",
            "Failed because of invalid quota size"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket_invalid_block_size(context: CliContext, #[future] bucket: String) {
        let args = create_bucket_cmd().try_get_matches_from(vec![
            "create",
            format!("local/{}", bucket.await).as_str(),
            "--block-size",
            "INVALID",
        ]);
        assert_eq!(
            args.err().unwrap().to_string(),
            "error: invalid value 'INVALID' for '--block-size <SIZE>'\n\nFor more information, try '--help'.\n",
            "Failed because of invalid block size"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket_invalid_block_records(
        context: CliContext,
        #[future] bucket: String,
    ) {
        let args = create_bucket_cmd().try_get_matches_from(vec![
            "create",
            format!("local/{}", bucket.await).as_str(),
            "--block-records",
            "INVALID",
        ]);
        assert_eq!(
            args.err().unwrap().to_string(),
            "error: invalid value 'INVALID' for '--block-records <NUMBER>': invalid digit found in string\n\nFor more information, try '--help'.\n",
            "Failed because of invalid block records"
        );
    }
}
