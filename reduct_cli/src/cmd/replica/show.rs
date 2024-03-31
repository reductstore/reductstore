// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::ALIAS_OR_URL_HELP;
use clap::{Arg, Command};

pub(super) fn ls_replica_cmd() -> Command {
    Command::new("show").about("Show replica details").arg(
        Arg::new("ALIAS_OR_URL")
            .help(ALIAS_OR_URL_HELP)
            .required(true),
    )
}

/*
rcli replication show test-storage stress_test

╭──────────── State ─────────────╮╭─────────── Settings ────────────╮
│ Name:                          ││ Source Bucket:                  │
│ stress_test                    ││ stress_test                     │
│ Active:                        ││ Destination Bucket:    demo     │
│ True                           ││ Destination Server:             │
│ Provisioned:                   ││ https://play.reduct.store/      │
│ True                           ││ Entries:               []       │
╰────────────────────────────────╯╰─────────────────────────────────╯
                          Errors last hour
┏━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Error Code ┃ Count ┃ Last Message                                 ┃
┡━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│        425 │ 1     │ Record with timestamp 1706741353382000 is    │
│            │       │ still being written                          │
└────────────┴───────┴──────────────────────────────────────────────┘
 */

pub(super) async fn show_replica(
    _ctx: &crate::context::CliContext,
    args: &clap::ArgMatches,
) -> anyhow::Result<()> {
    Ok(())
}
