// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use clap::Arg;

pub(crate) fn make_include_arg() -> Arg {
    Arg::new("include")
        .long("include")
        .short('I')
        .value_name("KEY=VALUE")
        .help("Only these records which have this key-value pair will be exported.\nThe format is key=value. This option can be used multiple times to include multiple key-value pairs.")
        .num_args(1..)
        .required(false)
}

pub(crate) fn make_exclude_arg() -> Arg {
    Arg::new("exclude")
        .long("exclude")
        .short('E')
        .value_name("KEY=VALUE")
        .help("These records which have this key-value pair will not be exported.\nThe format is key=value. This option can be used multiple times to exclude multiple key-value pairs.")
        .num_args(1..)
        .required(false)
}

pub(crate) fn make_entries_arg() -> Arg {
    Arg::new("entries")
        .long("entries")
        .short('n')
        .value_name("ENTRY_NAME")
        .help("List of entries to export.\nIf not specified, all entries will be exported. Wildcards are supported.")
        .num_args(1..)
        .required(false)
}
