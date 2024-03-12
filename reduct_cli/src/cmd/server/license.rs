// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::ALIAS_OR_URL_HELP;
use crate::io::std::output;
use clap::{Arg, ArgMatches, Command};

pub(super) fn server_license_cmd() -> Command {
    Command::new("license")
        .about("Get information about the license of a ReductStore instance")
        .arg(
            Arg::new("ALIAS_OR_URL")
                .help(ALIAS_OR_URL_HELP)
                .required(true),
        )
}

pub(super) async fn get_server_license(
    ctx: &crate::context::CliContext,
    args: &ArgMatches,
) -> anyhow::Result<()> {
    let alias = args.get_one::<String>("ALIAS_OR_URL").unwrap();

    let client = crate::io::reduct::build_client(ctx, alias).await?;

    if let Some(license) = client.server_info().await?.license {
        output!(ctx, "Licensee:\t{}", license.licensee);
        output!(ctx, "Invoice:\t{}", license.invoice);
        output!(ctx, "Expiry Date:\t{}", license.expiry_date);
        output!(ctx, "Plan:\t\t{}", license.plan);
        output!(ctx, "Num. Devices:\t{}", license.device_number);
        output!(ctx, "Disk Quota:\t{} TB", license.disk_quota);
        output!(ctx, "Fingerprint:\t{}", license.fingerprint);
    } else {
        output!(
            ctx,
            "BUSL-1.1 (Limited commercial use. See https://www.reduct.store/pricing for details)"
        );
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
    async fn test_get_server_license(context: crate::context::CliContext) {
        let matches = server_license_cmd().get_matches_from(vec!["license", "local"]);
        get_server_license(&context, &matches).await.unwrap();

        assert_eq!(context.stdout().history(), ["BUSL-1.1 (Limited commercial use. See https://www.reduct.store/pricing for details)"]);
    }
}
