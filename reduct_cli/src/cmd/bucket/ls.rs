// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::ALIAS_OR_URL_HELP;
use crate::context::CliContext;
use crate::helpers::timestamp_to_iso;
use crate::io::reduct::build_client;
use crate::io::std::output;

use bytesize::ByteSize;
use clap::ArgAction::SetTrue;
use clap::{Arg, ArgMatches, Command};
use colored::Colorize;
use reduct_rs::BucketInfoList;

pub(super) fn ls_bucket_cmd() -> Command {
    Command::new("ls")
        .about("list buckets")
        .arg(
            Arg::new("ALIAS_OR_URL")
                .help(ALIAS_OR_URL_HELP)
                .required(true),
        )
        .arg(
            Arg::new("full")
                .long("full")
                .short('f')
                .action(SetTrue)
                .help("Show full bucket information")
                .required(false),
        )
}

pub(super) async fn ls_bucket(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let alias_or_url = args.get_one::<String>("ALIAS_OR_URL").unwrap();
    let client = build_client(ctx, alias_or_url)?;

    let bucket_list = client.bucket_list().await?;
    if args.get_flag("full") {
        print_full_list(ctx, bucket_list);
    } else {
        print_list(ctx, bucket_list);
    }
    Ok(())
}

fn print_list(ctx: &CliContext, bucket_list: BucketInfoList) {
    for bucket in bucket_list.buckets {
        output!(ctx, "{}", bucket.name);
    }
}

fn print_full_list(ctx: &CliContext, bucket_list: BucketInfoList) {
    macro_rules! print_table {
        ($($x:expr),*) => {
            output!(ctx, "{:30}| {:10}| {:10} | {:30} | {:30}|", $($x),*);
        };
    }

    print_table!(
        "Name".bold(),
        "Entries".bold(),
        "Size".bold(),
        "Oldest record (UTC)".bold(),
        "Latest record (UTC)".bold()
    );
    print_table!(
        "-----------------------------",
        "----------",
        "----------",
        "------------------------------",
        "------------------------------"
    );
    for bucket in bucket_list.buckets {
        print_table!(
            bucket.name,
            bucket.entry_count,
            ByteSize(bucket.size).to_string(),
            timestamp_to_iso(bucket.oldest_record, bucket.entry_count == 0),
            timestamp_to_iso(bucket.latest_record, bucket.entry_count == 0)
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::{bucket, context};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_ls_bucket(context: CliContext, #[future] bucket: String) {
        let args = ls_bucket_cmd().get_matches_from(vec!["ls", "local"]);
        build_client(&context, "local")
            .unwrap()
            .create_bucket(&bucket.await)
            .send()
            .await
            .unwrap();

        ls_bucket(&context, &args).await.unwrap();
        assert!(context
            .stdout()
            .history()
            .contains(&"test_bucket".to_string()));
    }

    #[rstest]
    #[tokio::test]
    async fn test_ls_bucket_full(context: CliContext, #[future] bucket: String) {
        let args = ls_bucket_cmd().get_matches_from(vec!["ls", "local", "--full"]);
        let bucket = build_client(&context, "local")
            .unwrap()
            .create_bucket(&bucket.await)
            .send()
            .await
            .unwrap();
        bucket
            .write_record("test")
            .data("data".into())
            .timestamp_us(0)
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

        ls_bucket(&context, &args).await.unwrap();

        assert_eq!(context
                       .stdout()
                       .history(),
                   vec!["Name                          | Entries   | Size       | Oldest record (UTC)            | Latest record (UTC)           |",
                        "----------------------------- | ----------| ---------- | ------------------------------ | ------------------------------|",
                        "test_bucket                   |          1| 74 B       | 1970-01-01T00:00:00.000Z       | 1970-01-01T00:00:00.001Z      |"]
        );
    }
}
