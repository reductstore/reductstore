// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::RESOURCE_PATH_HELP;
use crate::context::CliContext;
use crate::io::reduct::build_client;
use crate::io::std::output;
use crate::parse::ResourcePathParser;
use clap::ArgAction::SetTrue;
use clap::{Arg, ArgMatches, Command};
use reduct_rs::{Permissions, ReductClient};

pub(super) fn create_token_cmd() -> Command {
    Command::new("create")
        .about("Create an access token")
        .arg(
            Arg::new("TOKEN_PATH")
                .help(RESOURCE_PATH_HELP)
                .value_parser(ResourcePathParser::new())
                .required(true),
        )
        .arg(
            Arg::new("full-access")
                .long("full-access")
                .short('A')
                .action(SetTrue)
                .help("Give full access to the token")
                .required(false),
        )
        .arg(
            Arg::new("read-bucket")
                .long("read-bucket")
                .short('r')
                .value_name("TEST")
                .num_args(1..)
                .help("Bucket to give read access to. Can be used multiple times")
                .required(false),
        )
        .arg(
            Arg::new("write-bucket")
                .long("write-bucket")
                .short('w')
                .value_name("TEST")
                .num_args(1..)
                .help("Bucket to give write access to. Can be used multiple times")
                .required(false),
        )
}

pub(super) async fn create_token(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let (alias_or_url, token_name) = args.get_one::<(String, String)>("TOKEN_PATH").unwrap();
    let full_access = args.get_flag("full-access");
    let read_buckets = args
        .get_many::<String>("read-bucket")
        .unwrap_or_default()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();
    let write_buckets = args
        .get_many::<String>("write-bucket")
        .unwrap_or_default()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

    let client: ReductClient = build_client(ctx, alias_or_url).await?;
    let token = client
        .create_token(
            token_name,
            Permissions {
                full_access,
                read: read_buckets,
                write: write_buckets,
            },
        )
        .await?;

    output!(ctx, "Token '{}' created: {}", token_name, token);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::{context, token};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_create_token(context: CliContext, #[future] token: String) {
        let args = create_token_cmd()
            .try_get_matches_from(vec![
                "create",
                format!("local/{}", token.await).as_str(),
                "--full-access",
                "--read-bucket",
                "test",
                "--write-bucket",
                "test",
            ])
            .unwrap();
        create_token(&context, &args).await.unwrap();
        assert!(
            context.stdout().history()[0].starts_with("Token 'test_token' created: test_token-")
        );
    }

    #[rstest]
    fn test_create_token_bad_path() {
        let cmd = create_token_cmd();
        let args = cmd.try_get_matches_from(vec!["create", "test"]);
        assert_eq!(args.unwrap_err().to_string(), "error: invalid value 'test' for '<TOKEN_PATH>'\n\nFor more information, try '--help'.\n");
    }
}
