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
    Command::new("show").about("Show replication details").arg(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::replica::tests::prepare_replication;
    use crate::context::tests::{bucket, bucket2, context, replica};
    use crate::context::CliContext;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_show_replica(
        context: CliContext,
        #[future] replica: String,
        #[future] bucket: String,
        #[future] bucket2: String,
    ) {
        let replica = replica.await;
        let bucket = bucket.await;
        let bucket2 = bucket2.await;

        prepare_replication(&context, &replica, &bucket, &bucket2)
            .await
            .unwrap();

        let args = show_replica_cmd()
            .get_matches_from(vec!["show", format!("local/{}", replica).as_str()]);
        build_client(&context, "local").await.unwrap();

        assert_eq!(show_replica_handler(&context, &args).await.unwrap(), ());
        assert_eq!(context.stdout().history(), vec!["Name:                test_replica         Source Bucket:         test_bucket",
                                                    "Active:              false                Destination Bucket:    test_bucket_2",
                                                    "Provisioned:         false                Destination Server:    http://localhost:8383",
                                                    "Pending Records:     0                    Entries:               []",
                                                    "Ok Records (hourly): 0                    Include:               {}",
                                                    "Errors (hourly):     0                    Exclude:               {}\n",
                                                    "| Error Code | Count | Last Message |\n|------------|-------|--------------|"]);
    }

    #[rstest]
    #[tokio::test]
    async fn test_show_replica_invalid_path() {
        let args = show_replica_cmd().try_get_matches_from(vec!["show", "local"]);

        assert_eq!(
            args.err().unwrap().to_string(),
            "error: invalid value 'local' for '<REPLICATION_PATH>'\n\nFor more information, try '--help'.\n"
        );
    }
}
