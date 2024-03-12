// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::ALIAS_OR_URL_HELP;
use crate::context::CliContext;
use crate::io::reduct::build_client;
use crate::io::std::output;
use clap::{Arg, ArgMatches, Command};

pub(super) fn ls_tokens_cmd() -> Command {
    Command::new("ls").about("List tokens").arg(
        Arg::new("ALIAS_OR_URL")
            .help(ALIAS_OR_URL_HELP)
            .required(true),
    )
}

pub(super) async fn ls_tokens(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let alias_or_url = args.get_one::<String>("ALIAS_OR_URL").unwrap();
    let client = build_client(ctx, alias_or_url).await?;

    let token_list = client.list_tokens().await?;
    for token in token_list {
        output!(ctx, "{}", token.name);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::{context, token};
    use reduct_rs::Permissions;
    use rstest::*;

    #[rstest]
    #[tokio::test]
    async fn test_ls_tokens(context: CliContext, #[future] token: String) {
        let token = token.await;
        let client = build_client(&context, "local").await.unwrap();
        client
            .create_token(&token, Permissions::default())
            .await
            .unwrap();

        let args = ls_tokens_cmd().get_matches_from(vec!["ls", "local"]);
        ls_tokens(&context, &args).await.unwrap();

        assert_eq!(
            context.stdout().history(),
            vec!["init-token", token.as_str()]
        );
    }
}
