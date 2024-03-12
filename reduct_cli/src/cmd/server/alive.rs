// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::ALIAS_OR_URL_HELP;
use crate::context::CliContext;

use crate::io::reduct::build_client;
use clap::{Arg, ArgMatches, Command};

pub(super) fn check_server_cmd() -> Command {
    Command::new("alive")
        .about("Check if a ReductStore instance is alive")
        .arg(
            Arg::new("ALIAS_OR_URL")
                .help(ALIAS_OR_URL_HELP)
                .required(true),
        )
}

pub(super) async fn check_server(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let alias_or_url = args.get_one::<String>("ALIAS_OR_URL").unwrap();
    let client = build_client(ctx, alias_or_url).await?;
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
        let args = check_server_cmd().get_matches_from(vec!["alive", "local"]);
        check_server(&context, &args).await.unwrap();
    }
}
