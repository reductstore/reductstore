// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::context::CliContext;

use crate::cmd::RESOURCE_PATH_HELP;
use crate::io::reduct::build_client;
use crate::io::std::output;
use crate::parse::ResourcePathParser;
use clap::ArgAction::SetTrue;
use clap::{Arg, ArgMatches, Command};
use reduct_rs::ReductClient;

pub(super) fn rm_bucket_cmd() -> Command {
    Command::new("rm")
        .about("Remove a bucket")
        .arg(
            Arg::new("BUCKET_PATH")
                .help(RESOURCE_PATH_HELP)
                .value_parser(ResourcePathParser::new())
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

    let confirm = if !args.get_flag("yes") {
        let confirm = dialoguer::Confirm::new()
            .default(false)
            .with_prompt(format!(
                "Are you sure you want to delete bucket '{}'?",
                bucket_name
            ))
            .interact()?;
        confirm
    } else {
        true
    };

    if confirm {
        let client: ReductClient = build_client(ctx, alias_or_url).await?;
        client.get_bucket(bucket_name).await?.remove().await?;

        output!(ctx, "Bucket '{}' deleted", bucket_name);
    } else {
        output!(ctx, "Bucket '{}' not deleted", bucket_name);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::{bucket, context};
    use reduct_rs::ErrorCode;
    use rstest::*;

    #[rstest]
    #[tokio::test]
    async fn test_rm_bucket(context: CliContext, #[future] bucket: String) {
        let bucket_name = bucket.await;
        let client = build_client(&context, "local").await.unwrap();
        client.create_bucket(&bucket_name).send().await.unwrap();

        let args = rm_bucket_cmd().get_matches_from(vec![
            "rm",
            format!("local/{}", bucket_name).as_str(),
            "--yes",
        ]);

        assert_eq!(rm_bucket(&context, &args).await.unwrap(), ());
        assert_eq!(
            client
                .get_bucket(&bucket_name)
                .await
                .err()
                .unwrap()
                .status(),
            ErrorCode::NotFound
        );
        assert_eq!(
            context.stdout().history(),
            vec!["Bucket 'test_bucket' deleted"]
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_rm_bucket_invalid_path() {
        let args = rm_bucket_cmd().try_get_matches_from(vec!["rm", "local"]);

        assert_eq!(
            args.err().unwrap().to_string(),
            "error: invalid value 'local' for '<BUCKET_PATH>'\n\nFor more information, try '--help'.\n"
        );
    }
}
