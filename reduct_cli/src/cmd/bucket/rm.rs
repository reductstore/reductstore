// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::bucket::{create_update_bucket_args, parse_bucket_settings};

use crate::context::CliContext;
use crate::reduct::build_client;

use crate::cmd::parsers::BucketPathParser;
use crate::cmd::BUCKET_PATH_HELP;
use clap::ArgAction::SetTrue;
use clap::{Arg, ArgMatches, Command};
use reduct_rs::ReductClient;

pub(super) fn rm_bucket_cmd() -> Command {
    Command::new("rm")
        .about("remove a bucket")
        .arg(
            Arg::new("BUCKET_PATH")
                .help(BUCKET_PATH_HELP)
                .value_parser(BucketPathParser::new())
                .required(true),
        )
        .arg(
            Arg::new("yes")
                .long("yes")
                .short('y')
                .action(SetTrue)
                .help("Do not ask for confirmation")
                .required(false),
        )
}

pub(super) async fn rm_bucket(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let (alias_or_url, bucket_name) = args.get_one::<(String, String)>("BUCKET_PATH").unwrap();

    if !args.get_flag("yes") {
        println!(
            "Are you sure you want to remove bucket '{}'? [N/y]",
            bucket_name
        );
        let mut confirmation = String::new();
        std::io::stdin().read_line(&mut confirmation)?;
        if confirmation.trim().to_lowercase() != "y" {
            println!("Aborting");
            return Ok(());
        }
    }

    let client: ReductClient = build_client(ctx, alias_or_url)?;
    client.get_bucket(bucket_name).await?.remove().await?;

    println!("Bucket '{}' removed", bucket_name);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::{bucket, context};
    use rstest::*;

    #[rstest]
    #[tokio::test]
    async fn test_rm_bucket(context: CliContext, #[future] bucket: String) {
        let bucket_name = bucket.await;
        let client = build_client(&context, "local").unwrap();
        client.create_bucket(&bucket_name).send().await.unwrap();

        let args = rm_bucket_cmd().get_matches_from(vec![
            "rm",
            format!("local/{}", bucket_name).as_str(),
            "--yes",
        ]);

        assert_eq!(rm_bucket(&context, &args).await.unwrap(), ());
    }

    #[rstest]
    #[tokio::test]
    async fn test_rm_bucket_invalid_path(context: CliContext) {
        let args = rm_bucket_cmd().try_get_matches_from(vec!["rm", "local"]);

        assert_eq!(
            args.err().unwrap().to_string(),
            "error: invalid value 'local' for '<BUCKET_PATH>'\n\nFor more information, try '--help'.\n"
        );
    }
}
