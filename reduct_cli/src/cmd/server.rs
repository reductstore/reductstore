// Copyright 2023-2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod alive;
mod license;
mod status;

use crate::context::CliContext;
use clap::Command;

pub(crate) fn server_cmd() -> Command {
    Command::new("server")
        .about("Get information about a ReductStore instance")
        .arg_required_else_help(true)
        .subcommand(alive::check_server_cmd())
        .subcommand(status::server_status_cmd())
        .subcommand(license::server_license_cmd())
}

pub(crate) async fn server_handler(
    ctx: &CliContext,
    matches: Option<(&str, &clap::ArgMatches)>,
) -> anyhow::Result<()> {
    match matches {
        Some(("alive", args)) => alive::check_server(ctx, args).await?,
        Some(("status", args)) => status::get_server_status(ctx, args).await?,
        Some(("license", args)) => license::get_server_license(ctx, args).await?,
        _ => (),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::context;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_server_handler_alive(context: CliContext) {
        let matches = server_cmd().get_matches_from(vec!["server", "alive", "default"]);

        assert!(
            server_handler(&context, matches.subcommand())
                .await
                .is_err(),
            "Check handling of server alive command"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_server_handler_status(context: CliContext) {
        let matches = server_cmd().get_matches_from(vec!["server", "status", "default"]);

        assert!(
            server_handler(&context, matches.subcommand())
                .await
                .is_err(),
            "Check handling of server status command"
        );
    }
}
