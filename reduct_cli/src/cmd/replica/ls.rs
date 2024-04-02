// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::ALIAS_OR_URL_HELP;
use crate::io::std::output;
use clap::ArgAction::SetTrue;
use clap::{Arg, Command};

use reduct_rs::ReplicationInfo;
use tabled::{settings::Style, Table, Tabled};

pub(super) fn ls_replica_cmd() -> Command {
    Command::new("ls")
        .about("List replications")
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
                .help("Show full replication information")
                .required(false),
        )
}

#[derive(Tabled)]
struct ReplicationTable {
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "State")]
    state: String,
    #[tabled(rename = "Pending Records")]
    pending_records: u64,
    #[tabled(rename = "Provisioned")]
    is_provisioned: bool,
}

impl From<ReplicationInfo> for ReplicationTable {
    fn from(replication: ReplicationInfo) -> Self {
        Self {
            name: replication.name,
            state: if replication.is_active {
                "Active".to_string()
            } else {
                "Inactive".to_string()
            },
            pending_records: replication.pending_records,
            is_provisioned: replication.is_provisioned,
        }
    }
}

pub(super) async fn ls_replica(
    _ctx: &crate::context::CliContext,
    args: &clap::ArgMatches,
) -> anyhow::Result<()> {
    let alias_or_url = args.get_one::<String>("ALIAS_OR_URL").unwrap();
    let client = crate::io::reduct::build_client(_ctx, alias_or_url).await?;

    let print_list = |ctx: &crate::context::CliContext, replication_list: Vec<ReplicationInfo>| {
        for replication in replication_list {
            output!(ctx, "{}", replication.name);
        }
    };

    let print_full_list = |ctx: &crate::context::CliContext,
                           replication_list: Vec<ReplicationInfo>| {
        let table = Table::new(replication_list.into_iter().map(ReplicationTable::from))
            .with(Style::markdown())
            .to_string();
        output!(ctx, "{}", table);
    };

    if args.get_flag("full") {
        print_full_list(_ctx, client.list_replications().await?);
    } else {
        print_list(_ctx, client.list_replications().await?);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::context::tests::{bucket, bucket2, context, replica};

    use crate::cmd::replica::tests::prepare_replication;
    use crate::context::CliContext;

    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_list_replications(
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

        let args = ls_replica_cmd()
            .try_get_matches_from(vec!["ls", "local"])
            .unwrap();

        ls_replica(&context, &args).await.unwrap();
        assert_eq!(context.stdout().history(), vec![replica]);
    }

    #[rstest]
    #[tokio::test]
    async fn test_list_replications_full(
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

        let args = ls_replica_cmd()
            .try_get_matches_from(vec!["ls", "local", "--full"])
            .unwrap();

        ls_replica(&context, &args).await.unwrap();
        assert_eq!(
            context.stdout().history(),
            vec![
                "| Name         | State    | Pending Records | Provisioned |\n\
            |--------------|----------|-----------------|-------------|\n\
            | test_replica | Inactive | 0               | false       |"
            ]
        );
    }
}
