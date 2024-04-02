// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::RESOURCE_PATH_HELP;
use crate::context::CliContext;
use crate::io::reduct::build_client;
use crate::io::std::output;
use clap::ArgAction::SetTrue;
use clap::{Arg, ArgMatches, Command};

pub(super) fn rm_replica_cmd() -> Command {
    Command::new("rm")
        .about("Remove a replication")
        .arg(
            Arg::new("REPLICATION_PATH")
                .help(RESOURCE_PATH_HELP)
                .value_parser(crate::parse::ResourcePathParser::new())
                .required(true),
        )
        .arg(
            Arg::new("yes")
                .long("yes")
                .short('y')
                .help("Do not ask for confirmation")
                .action(SetTrue)
                .required(false),
        )
}

pub(super) async fn rm_replica_handler(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let (alias_or_url, replication_name) = args
        .get_one::<(String, String)>("REPLICATION_PATH")
        .unwrap();

    let confirm = if !args.get_flag("yes") {
        let confirm = dialoguer::Confirm::new()
            .default(false)
            .with_prompt(format!(
                "Are you sure you want to delete replication '{}'?",
                replication_name
            ))
            .interact()?;
        confirm
    } else {
        true
    };

    if confirm {
        let client = build_client(ctx, alias_or_url).await?;
        client.delete_replication(replication_name).await?;
        output!(ctx, "Replication '{}' deleted", replication_name);
    } else {
        output!(ctx, "Replication '{}' not deleted", replication_name);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::cmd::replica::rm::{rm_replica_cmd, rm_replica_handler};
    use crate::cmd::replica::tests::prepare_replication;
    use crate::context::tests::{bucket, bucket2, context, replica};
    use crate::context::CliContext;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_rm_replica(
        context: CliContext,
        #[future] replica: String,
        #[future] bucket: String,
        #[future] bucket2: String,
    ) {
        let replica = replica.await;
        let bucket = bucket.await;
        let bucket2 = bucket2.await;

        let client = prepare_replication(&context, &replica, &bucket, &bucket2)
            .await
            .unwrap();

        let args = rm_replica_cmd()
            .try_get_matches_from(vec!["rm", format!("local/{}", replica).as_str(), "--yes"])
            .unwrap();
        rm_replica_handler(&context, &args).await.unwrap();

        assert_eq!(
            client
                .get_replication(&replica)
                .await
                .err()
                .unwrap()
                .to_string(),
            "[NotFound] Replication 'test_replica' does not exist"
        );
    }
}
