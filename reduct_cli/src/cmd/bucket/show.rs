// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::RESOURCE_PATH_HELP;
use crate::context::CliContext;
use crate::helpers::timestamp_to_iso;
use crate::io::reduct::build_client;
use crate::io::std::output;
use crate::parsers::BucketPathParser;
use bytesize::ByteSize;
use clap::ArgAction::SetTrue;
use clap::{Arg, ArgMatches, Command};
use colored::*;
use reduct_rs::{BucketInfo, FullBucketInfo, ReductClient};

pub(super) fn show_bucket_cmd() -> Command {
    Command::new("show")
        .about("Show bucket information")
        .arg(
            Arg::new("BUCKET_PATH")
                .help(RESOURCE_PATH_HELP)
                .value_parser(BucketPathParser::new())
                .required(true),
        )
        .arg(
            Arg::new("full")
                .long("full")
                .short('f')
                .action(SetTrue)
                .help("Show full bucket information with entries")
                .required(false),
        )
}

pub(super) async fn show_bucket(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let (alias_or_url, bucket_name) = args.get_one::<(String, String)>("BUCKET_PATH").unwrap();

    let client: ReductClient = build_client(ctx, alias_or_url).await?;
    let bucket = client.get_bucket(bucket_name).await?.full_info().await?;

    if args.get_flag("full") {
        print_full_bucket(ctx, bucket)?;
    } else {
        print_bucket(ctx, bucket.info)?;
    }

    Ok(())
}

fn print_bucket(ctx: &CliContext, bucket: BucketInfo) -> anyhow::Result<()> {
    output!(ctx, "Name:                {}", bucket.name);
    output!(ctx, "Entries:             {}", bucket.entry_count);
    output!(ctx, "Size:                {}", ByteSize(bucket.size));
    output!(
        ctx,
        "Oldest Record (UTC): {}",
        timestamp_to_iso(bucket.oldest_record, bucket.entry_count == 0)
    );
    output!(
        ctx,
        "Latest Record (UTC): {}",
        timestamp_to_iso(bucket.latest_record, bucket.entry_count == 0)
    );
    Ok(())
}

fn print_full_bucket(ctx: &CliContext, bucket: FullBucketInfo) -> anyhow::Result<()> {
    let settings = bucket.settings;
    let info = bucket.info;
    output!(
        ctx,
        "Name:                {:30} Quota Type:         {}",
        info.name,
        settings.quota_type.unwrap()
    );
    output!(
        ctx,
        "Entries:             {:<30} Quota Size:         {}",
        info.entry_count,
        ByteSize(settings.quota_size.unwrap())
    );
    output!(
        ctx,
        "Size:                {:30} Max. Block Size:    {}",
        ByteSize(info.size),
        ByteSize(settings.max_block_size.unwrap())
    );
    output!(
        ctx,
        "Oldest Record (UTC): {:30} Max. Block Records: {}",
        timestamp_to_iso(info.oldest_record, info.entry_count == 0),
        settings.max_block_records.unwrap()
    );
    output!(
        ctx,
        "Latest Record (UTC): {:30}",
        timestamp_to_iso(info.latest_record, info.entry_count == 0)
    );
    macro_rules! print_table {
        ($($x:expr),*) => {
            output!(ctx, "{:30}| {:10}| {:10}| {:10} | {:30} | {:30}|", $($x),*);
        };
    }

    print_table!(
        "-----------------------------",
        "----------",
        "----------",
        "----------",
        "------------------------------",
        "------------------------------"
    );

    print_table!(
        "Name".bold(),
        "Blocks".bold(),
        "Records".bold(),
        "Size".bold(),
        "Oldest record (UTC)".bold(),
        "Latest record (UTC)".bold()
    );
    print_table!(
        "-----------------------------",
        "----------",
        "----------",
        "----------",
        "------------------------------",
        "------------------------------"
    );
    for entry in bucket.entries {
        print_table!(
            entry.name,
            entry.block_count,
            entry.record_count,
            ByteSize(entry.size).to_string(),
            timestamp_to_iso(entry.oldest_record, entry.record_count == 0),
            timestamp_to_iso(entry.latest_record, entry.record_count == 0)
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::{bucket, context};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_show_bucket(context: CliContext, #[future] bucket: String) {
        let bucket_name = bucket.await;
        let client = build_client(&context, "local").await.unwrap();
        client.create_bucket(&bucket_name).send().await.unwrap();

        let args = show_bucket_cmd()
            .get_matches_from(vec!["show", format!("local/{}", bucket_name).as_str()]);

        assert_eq!(show_bucket(&context, &args).await.unwrap(), ());
        assert_eq!(
            context.stdout().history(),
            vec![
                "Name:                test_bucket",
                "Entries:             0",
                "Size:                0 B",
                "Oldest Record (UTC): ---",
                "Latest Record (UTC): ---",
            ]
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_show_bucket_full(context: CliContext, #[future] bucket: String) {
        let bucket_name = bucket.await;
        let client = build_client(&context, "local").await.unwrap();
        let bucket = client.create_bucket(&bucket_name).send().await.unwrap();
        bucket
            .write_record("test")
            .data("data".into())
            .timestamp_us(1)
            .send()
            .await
            .unwrap();
        bucket
            .write_record("test")
            .data("data".into())
            .timestamp_us(1000)
            .send()
            .await
            .unwrap();

        let args = show_bucket_cmd().get_matches_from(vec![
            "show",
            format!("local/{}", bucket_name).as_str(),
            "--full",
        ]);

        assert_eq!(show_bucket(&context, &args).await.unwrap(), ());
        assert_eq!(
            context.stdout().history(),
            vec!["Name:                test_bucket                    Quota Type:         NONE",
                 "Entries:             1                              Quota Size:         0 B",
                 "Size:                99 B                           Max. Block Size:    64.0 MB",
                 "Oldest Record (UTC): 1970-01-01T00:00:00.000Z       Max. Block Records: 256",
                 "Latest Record (UTC): 1970-01-01T00:00:00.001Z      ",
                 "----------------------------- | ----------| ----------| ---------- | ------------------------------ | ------------------------------|",
                 "Name                          | Blocks    | Records   | Size       | Oldest record (UTC)            | Latest record (UTC)           |",
                 "----------------------------- | ----------| ----------| ---------- | ------------------------------ | ------------------------------|",
                 "test                          |          1|          2| 99 B       | 1970-01-01T00:00:00.000Z       | 1970-01-01T00:00:00.001Z      |"]
        );
    }
}
