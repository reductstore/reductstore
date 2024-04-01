// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::RESOURCE_PATH_HELP;
use crate::context::CliContext;
use crate::io::reduct::{build_client, parse_url_and_token};
use crate::parse::widely_used_args::{
    make_entries_arg, make_exclude_arg, make_include_arg, parse_label_args,
};
use crate::parse::ResourcePathParser;

use clap::{Arg, ArgMatches, Command};
use reduct_rs::ReplicationSettings;

pub(super) fn update_replica_cmd() -> Command {
    Command::new("update")
        .about("Update a replication")
        .arg(
            Arg::new("REPLICATION_PATH")
                .help(RESOURCE_PATH_HELP)
                .value_parser(ResourcePathParser::new())
                .required(true),
        )
        .arg(
            Arg::new("source-bucket")
                .short('s')
                .help("Source bucket on the replicated instance")
                .required(false),
        )
        .arg(
            Arg::new("dest-path")
                .short('D')
                .help(RESOURCE_PATH_HELP)
                .value_parser(ResourcePathParser::new())
                .required(false),
        )
        .arg(make_include_arg())
        .arg(make_exclude_arg())
        .arg(make_entries_arg())
}

pub(super) async fn update_replica_handler(
    ctx: &CliContext,
    args: &ArgMatches,
) -> anyhow::Result<()> {
    let (alias_or_url, replication_name) = args
        .get_one::<(String, String)>("REPLICATION_PATH")
        .unwrap();

    let client = build_client(ctx, &alias_or_url).await?;
    let current_settings = client.get_replication(&replication_name).await?.settings;
    let new_settings = update_replication_settings(ctx, args, current_settings)?;

    client
        .update_replication(&replication_name, new_settings)
        .await?;
    Ok(())
}

fn update_replication_settings(
    ctx: &CliContext,
    args: &ArgMatches,
    mut current_settings: ReplicationSettings,
) -> anyhow::Result<ReplicationSettings> {
    let source_bucket_name = args.get_one::<String>("source-bucket");
    let dest_path = args.get_one::<(String, String)>("dest-path");

    let entries_filter = args
        .get_many::<String>("entries")
        .map(|s| s.map(|s| s.to_string()).collect::<Vec<String>>());

    let include = parse_label_args(args.get_many::<String>("include"))?;
    let exclude = parse_label_args(args.get_many::<String>("exclude"))?;

    if let Some(source_bucket_name) = source_bucket_name {
        current_settings.src_bucket = source_bucket_name.clone();
    }

    if let Some(dest_path) = dest_path {
        let (dest_alias_or_url, dest_bucket_name) = dest_path;
        let (dest_url, token) = parse_url_and_token(ctx, &dest_alias_or_url)?;
        current_settings.dst_bucket = dest_bucket_name.clone();
        current_settings.dst_host = dest_url.to_string();
        current_settings.dst_token = token.clone();
    }

    if let Some(include) = include {
        current_settings.include = include;
    }

    if let Some(exclude) = exclude {
        current_settings.exclude = exclude;
    }

    if let Some(entries_filter) = entries_filter {
        current_settings.entries = entries_filter;
    }
    Ok(current_settings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::replica::tests::prepare_replication;
    use crate::context::tests::{bucket, bucket2, context, replica};
    use reduct_rs::Labels;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_update_replica(
        context: CliContext,
        #[future] replica: String,
        #[future] bucket: String,
        #[future] bucket2: String,
    ) {
        let test_replica = replica.await;
        let bucket = bucket.await;
        let bucket2 = bucket2.await;

        let client = prepare_replication(&context, &test_replica, &bucket, &bucket2)
            .await
            .unwrap();

        let args = update_replica_cmd()
            .try_get_matches_from(vec![
                "create",
                format!("local/{}", test_replica).as_str(),
                "--include",
                "key1=value2",
            ])
            .unwrap();

        update_replica_handler(&context, &args).await.unwrap();

        let replica = client.get_replication(&test_replica).await.unwrap();
        assert_eq!(replica.settings.src_bucket, bucket);
        assert_eq!(replica.settings.dst_bucket, bucket2);
        assert_eq!(replica.settings.dst_host, "http://localhost:8383");
        assert_eq!(replica.settings.dst_token, "***");
        assert_eq!(
            replica.settings.include,
            Labels::from_iter(vec![("key1".to_string(), "value2".to_string())])
        );
        assert!(replica.settings.exclude.is_empty());
        assert!(replica.settings.entries.is_empty());
    }

    mod update_settings {
        // TODO
    }
}
