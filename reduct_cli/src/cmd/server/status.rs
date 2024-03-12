// Copyright 2023-2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::ALIAS_OR_URL_HELP;
use crate::io::reduct::build_client;
use crate::io::std::output;
use clap::{Arg, ArgMatches, Command};
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
    args: &ArgMatches,
) -> anyhow::Result<()> {
    let alias = args.get_one::<String>("ALIAS_OR_URL").unwrap();

    let client = build_client(ctx, alias).await?;
    let info = client.server_info().await?;

    output!(ctx, "Status: \tOk");
    output!(ctx, "Version:\t{}", info.version);
    output!(
        ctx,
        "Uptime: \t{}",
        HumanTime::from_seconds(info.uptime as i64).to_text_en(Accuracy::Rough, Tense::Present)
    );

    if let Some(license) = info.license {
        output!(
            ctx,
            "License:\t{}, expired at {}",
            license.plan,
            license.expiry_date
        );
    } else {
        output!(ctx, "License:\tBUSL-1.1 (Limited commercial use)");
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
    async fn test_get_server_status(context: crate::context::CliContext) {
        let args = server_status_cmd().get_matches_from(vec!["status", "local"]);
        get_server_status(&context, &args).await.unwrap();
        assert_eq!(context.stdout().history().len(), 4);
        assert_eq!(context.stdout().history()[0], "Status: \tOk");
        assert_eq!(
            context.stdout().history()[1],
            format!("Version:\t{}", env!("CARGO_PKG_VERSION"))
        );
        assert!(context.stdout().history()[2].starts_with("Uptime: \t"));
        assert_eq!(
            context.stdout().history()[3],
            "License:\tBUSL-1.1 (Limited commercial use)"
        );
    }
}
