// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::find_alias;
use crate::context::CliContext;
use clap::{arg, crate_name, Command};
use reduct_rs::ReductClient;

pub(super) fn check_server_cmd() -> Command {
    Command::new("alive")
        .about("Check if a ReductStore instance is alive")
        .arg(arg!(<ALIAS> "Alias of the ReductStore instance to check").required(true))
}

pub(super) async fn check_server(ctx: &CliContext, alias: &str) -> anyhow::Result<()> {
    let alias = find_alias(ctx, alias)?;

    let client = ReductClient::builder()
        .url(alias.url.as_str())
        .api_token(alias.token.as_str())
        .build();
    client.alive().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::context;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_check_server(context: CliContext) {
        check_server(&context, "local").await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn test_check_server_invalid_alias(context: CliContext) {
        let result = check_server(&context, "invalid").await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Alias 'invalid' does not exist"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_check_server_no_connection(context: CliContext) {
        let result = check_server(&context, "default").await;
        assert!(result.is_err());
    }
}
