// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use clap::{arg, Command};

pub(super) fn add_alias_cmd() -> Command {
    Command::new("rm")
        .about("Remove an alias")
        .arg(arg!(<NAME> "The name of the alias to remove").required(true))
}
