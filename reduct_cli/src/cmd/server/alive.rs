// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::context::CliContext;
use crate::reduct::build_client;
use clap::{arg, Command};

pub(super) fn check_server_cmd() -> Command {
    Command::new("alive")
        .about("Check if a ReductStore instance is alive")
        .arg(
            arg!(<ALIAS_OR_URL> "Alias or URL of the ReductStore instance to check").required(true),
        )
}

pub(super) async fn check_server(ctx: &CliContext, alias_or_url: &str) -> anyhow::Result<()> {
    let client = build_client(ctx, alias_or_url)?;
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
}
