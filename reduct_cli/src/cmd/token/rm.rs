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

pub(super) fn rm_token_cmd() -> Command {
    Command::new("rm")
        .about("Remove a new access token")
        .arg(
            Arg::new("TOKEN_PATH")
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
        .arg_required_else_help(true)
}

pub(super) async fn rm_token(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let (alias_or_url, token_name) = args.get_one::<(String, String)>("TOKEN_PATH").unwrap();

    let confirm = if !args.get_flag("yes") {
        let confirm = dialoguer::Confirm::new()
            .default(false)
            .with_prompt(format!(
                "Are you sure you want to delete the token '{}'?",
                token_name
            ))
            .interact()?;
        confirm
    } else {
        true
    };

    if confirm {
        let client = build_client(ctx, alias_or_url).await?;
        client.delete_token(token_name).await?;
        output!(ctx, "Token '{}' deleted", token_name);
    } else {
        output!(ctx, "Token '{}' not deleted", token_name);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::token::create::create_token_cmd;
    use crate::context::tests::{context, token};
    use reduct_rs::ErrorCode;
    use rstest::*;

    #[rstest]
    #[tokio::test]
    async fn test_rm_token(context: CliContext, #[future] token: String) {
        let token = token.await;
        let client = build_client(&context, "local").await.unwrap();
        client
            .create_token(&token, Default::default())
            .await
            .unwrap();

        let args = rm_token_cmd().get_matches_from(vec![
            "rm",
            format!("local/{}", token).as_str(),
            "--yes",
        ]);
        assert_eq!(rm_token(&context, &args).await.unwrap(), ());
        assert_eq!(
            client.get_token(&token).await.unwrap_err().status(),
            ErrorCode::NotFound
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_rm_token_bad_path() {
        let cmd = create_token_cmd();
        let args = cmd.try_get_matches_from(vec!["create", "test"]);
        assert_eq!(args.unwrap_err().to_string(), "error: invalid value 'test' for '<TOKEN_PATH>'\n\nFor more information, try '--help'.\n");
    }
}
