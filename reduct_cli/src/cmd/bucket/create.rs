// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::BUCKET_PATH_HELP;
use crate::context::CliContext;
use clap::{Arg, ArgMatches, Command};

pub(super) fn create_bucket_cmd() -> Command {
    Command::new("create")
        .about("create a bucket")
        .arg(
            Arg::new("BUCKET_PATH")
                .help(BUCKET_PATH_HELP)
                .required(true),
        )
        .arg(
            Arg::new("quota-type")
                .long("quota-type")
                .short('Q')
                .value_name("TEXT")
                .help("Quota type. Must be NONE or FIFO")
                .required(false),
        )
        .arg(
            Arg::new("quota-size")
                .long("quota-size")
                .short('s')
                .value_name("TEXT")
                .help("Quota size in CI format e.g. 1Mb or 3TB"),
        )
        .arg(
            Arg::new("block-size")
                .long("block-size")
                .short('b')
                .value_name("TEXT")
                .help("Max. bock size in CI format e.g 64MB"),
        )
        .arg(
            Arg::new("block-records")
                .long("block-records")
                .short('R')
                .value_name("NUMBER")
                .help("Max. number of records in a block"),
        )
}

pub(super) fn crate_bucket(_ctx: &CliContext, _args: &ArgMatches) -> anyhow::Result<()> {
    Ok(())
}
