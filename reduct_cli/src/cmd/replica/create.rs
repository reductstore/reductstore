// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::RESOURCE_PATH_HELP;
use crate::io::reduct::{build_client, parse_url_and_token};
use crate::parse::widely_used_args::{
    make_entries_arg, make_exclude_arg, make_include_arg, parse_label_args,
};
use crate::parse::ResourcePathParser;
use clap::{Arg, Command};
use reduct_rs::Labels;

pub(super) fn create_replica_cmd() -> Command {
    Command::new("create")
        .about("Create a replication between two buckets")
        .arg(
            Arg::new("REPLICATION_PATH")
                .help(RESOURCE_PATH_HELP)
                .value_parser(ResourcePathParser::new())
                .required(true),
        )
        .arg(
            Arg::new("SOURCE_BUCKET_NAME")
                .help("Source bucket on the replicated instance")
                .required(true),
        )
        .arg(
            Arg::new("DEST_BUCKET_PATH")
                .help(RESOURCE_PATH_HELP)
                .required(true)
                .value_parser(ResourcePathParser::new())
                .required(true),
        )
        .arg(make_include_arg())
        .arg(make_exclude_arg())
        .arg(make_entries_arg())
}

pub(super) async fn create_replica(
    ctx: &crate::context::CliContext,
    args: &clap::ArgMatches,
) -> anyhow::Result<()> {
    let (alias_or_url, replication_name) = args
        .get_one::<(String, String)>("REPLICATION_PATH")
        .unwrap();
    let source_bucket_name = args.get_one::<String>("SOURCE_BUCKET_NAME").unwrap();
    let (dest_alias_or_url, dest_bucket_name) = args
        .get_one::<(String, String)>("DEST_BUCKET_PATH")
        .unwrap();
    let entries_filter = args
        .get_many::<String>("entries")
        .unwrap_or_default()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

    let include = parse_label_args(args.get_many::<String>("include"))?.unwrap_or_default();
    let exclude = parse_label_args(args.get_many::<String>("exclude"))?.unwrap_or_default();

    let client = build_client(ctx, &alias_or_url).await?;
    let (dest_url, token) = parse_url_and_token(ctx, &dest_alias_or_url)?;

    client
        .create_replication(replication_name)
        .src_bucket(source_bucket_name)
        .dst_bucket(dest_bucket_name)
        .dst_host(dest_url.as_str())
        .dst_token(&token)
        .include(Labels::from_iter(include))
        .exclude(Labels::from_iter(exclude))
        .entries(entries_filter)
        .send()
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::{bucket, bucket2, context, replica};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_create_replica(
        context: crate::context::CliContext,
        #[future] replica: String,
        #[future] bucket: String,
        #[future] bucket2: String,
    ) {
        let test_replica = replica.await;
        let bucket = bucket.await;
        let bucket2 = bucket2.await;

        let client = build_client(&context, "local").await.unwrap();
        client.create_bucket(&bucket).send().await.unwrap();
        client.create_bucket(&bucket2).send().await.unwrap();

        let args = create_replica_cmd().get_matches_from(vec![
            "create",
            format!("local/{}", test_replica).as_str(),
            &bucket,
            format!("local/{}", bucket2).as_str(),
            "--include",
            "key1=value2",
            "--exclude",
            "key2=value3",
            "--entries",
            "entry1",
            "entry2",
        ]);
        create_replica(&context, &args).await.unwrap();

        let replica = client.get_replication(&test_replica).await.unwrap();
        assert_eq!(replica.settings.src_bucket, bucket);
        assert_eq!(replica.settings.dst_bucket, bucket2);
        assert_eq!(replica.settings.dst_host, "http://localhost:8383/");
        assert_eq!(replica.settings.dst_token, "***");
        assert_eq!(
            replica.settings.include,
            Labels::from_iter(vec![("key1".to_string(), "value2".to_string())])
        );
        assert_eq!(
            replica.settings.exclude,
            Labels::from_iter(vec![("key2".to_string(), "value3".to_string())])
        );
        assert_eq!(
            replica.settings.entries,
            vec!["entry1".to_string(), "entry2".to_string()]
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_replica_with_invalid_alias() {
        let args = create_replica_cmd().try_get_matches_from(vec![
            "create",
            "invalid",
            "test_bucket",
            "local/test_bucket_2",
        ]);
        assert!(args.is_err());
    }
}
