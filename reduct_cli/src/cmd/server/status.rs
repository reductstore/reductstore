// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::ALIAS_OR_URL_HELP;
use crate::reduct::build_client;
use clap::{arg, Arg, Command};
use time_humanize::{Accuracy, HumanTime, Tense};

pub(super) fn server_status_cmd() -> Command {
    Command::new("status")
        .about("Get the status of a ReductStore instance")
        .arg(
            Arg::new("ALIAS_OR_URL")
                .help(ALIAS_OR_URL_HELP)
                .required(true),
        )
}

pub(super) async fn get_server_status(
    ctx: &crate::context::CliContext,
    alias: &str,
) -> anyhow::Result<()> {
    let client = build_client(ctx, alias)?;
    let info = client.server_info().await?;

    ctx.output().print("Status: \tOk");
    ctx.output().print(&format!("Version:\t{}", info.version));
    ctx.output().print(&format!(
        "Uptime: \t{}",
        HumanTime::from_seconds(info.uptime as i64).to_text_en(Accuracy::Rough, Tense::Present)
    ));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::context;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_get_server_status(context: crate::context::CliContext) {
        get_server_status(&context, "local").await.unwrap();
        assert_eq!(context.output().history().len(), 3);
        assert_eq!(context.output().history()[0], "Status: \tOk");
        assert_eq!(
            context.output().history()[1],
            format!("Version:\t{}", env!("CARGO_PKG_VERSION"))
        );
        assert!(context.output().history()[2].starts_with("Uptime: \t"));
    }
}
