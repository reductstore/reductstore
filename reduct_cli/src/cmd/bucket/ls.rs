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

use reduct_rs::{BucketInfo, BucketInfoList};
use tabled::settings::Style;
use tabled::{Table, Tabled};

pub(super) fn ls_bucket_cmd() -> Command {
    Command::new("ls")
        .about("List buckets")
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
    let client = build_client(ctx, alias_or_url).await?;

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

#[derive(Tabled)]
struct BucketTable {
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Entries")]
    entry_count: u64,
    #[tabled(rename = "Size")]
    size: ByteSize,
    #[tabled(rename = "Oldest record (UTC)")]
    oldest_record: String,
    #[tabled(rename = "Latest record (UTC)")]
    latest_record: String,
}

impl From<BucketInfo> for BucketTable {
    fn from(bucket: BucketInfo) -> Self {
        Self {
            name: bucket.name,
            entry_count: bucket.entry_count,
            size: ByteSize(bucket.size),
            oldest_record: timestamp_to_iso(bucket.oldest_record, bucket.entry_count == 0),
            latest_record: timestamp_to_iso(bucket.latest_record, bucket.entry_count == 0),
        }
    }
}

fn print_full_list(ctx: &CliContext, bucket_list: BucketInfoList) {
    let table = Table::new(bucket_list.buckets.into_iter().map(BucketTable::from))
        .with(Style::markdown())
        .to_string();
    output!(ctx, "{}", table);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::{bucket, bucket2, context};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_ls_bucket(context: CliContext, #[future] bucket: String) {
        let args = ls_bucket_cmd().get_matches_from(vec!["ls", "local"]);
        build_client(&context, "local")
            .await
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
    async fn test_ls_bucket_full(
        context: CliContext,
        #[future] bucket: String,
        #[future] bucket2: String,
    ) {
        let args = ls_bucket_cmd().get_matches_from(vec!["ls", "local", "--full"]);
        let client = build_client(&context, "local").await.unwrap();
        let bucket = client.create_bucket(&bucket.await).send().await.unwrap();
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
        client.create_bucket(&bucket2.await).send().await.unwrap();

        ls_bucket(&context, &args).await.unwrap();

        assert_eq!(context
                       .stdout()
                       .history(),
                   vec!["| Name          | Entries | Size | Oldest record (UTC)      | Latest record (UTC)      |\n\
                   |---------------|---------|------|--------------------------|--------------------------|\n\
                   | test_bucket   | 1       | 90 B | 1970-01-01T00:00:00.000Z | 1970-01-01T00:00:00.001Z |\n\
                   | test_bucket_2 | 0       | 0 B  | ---                      | ---                      |"]
        );
    }
}
