// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod b2b;
mod b2f;
mod helpers;

use crate::cmd::cp::b2b::cp_bucket_to_bucket;
use crate::cmd::cp::b2f::cp_bucket_to_folder;
use crate::cmd::ALIAS_OR_URL_HELP;
use crate::context::CliContext;
use crate::parsers::ResourcePathParser;
use clap::ArgAction::{SetFalse, SetTrue};
use clap::{value_parser, Arg, Command};
use url::Url;

pub(crate) fn cp_cmd() -> Command {
    Command::new("cp")
        .about("Copy data between ReductStore instances or between a ReductStore instance and the local filesystem")
        .arg_required_else_help(true)
        .arg(
            Arg::new("SOURCE_BUCKET_OR_FOLDER")
                .help(ALIAS_OR_URL_HELP)
                .value_parser(ResourcePathParser::new())
                .required(true),
        )
        .arg(
            Arg::new("DESTINATION_BUCKET_OR_FOLDER")
                .help(ALIAS_OR_URL_HELP)
                .value_parser(ResourcePathParser::new())
                .required(true),
        )
        .arg(
            Arg::new("start")
                .long("start")
                .short('b')
                .help("Export records  with timestamps older than this time point in ISO format or Unix timestamp in microseconds.\nIf not specified, the export will start from the first record in an entry.")
                .required(false)
        )
        .arg(
            Arg::new("stop")
                .long("stop")
                .short('e')
                .help("Export records with timestamps newer than this time point in ISO format or Unix timestamp in microseconds.\nIf not specified, the export will end at the last record in an entry.")
                .required(false)
        )
        .arg(
            Arg::new("include")
                .long("include")
                .short('I')
                .value_name("KEY=VALUE")
                .help("Only these records which have this key-value pair will be exported.\nThe format is key=value. This option can be used multiple times to include multiple key-value pairs.")
                .num_args(1..)
                .required(false)
        )
        .arg(
            Arg::new("exclude")
                .long("exclude")
                .short('E')
                .value_name("KEY=VALUE")
                .help("These records which have this key-value pair will not be exported.\nThe format is key=value. This option can be used multiple times to exclude multiple key-value pairs.")
                .num_args(1..)
                .required(false)
        )
        .arg(Arg::new("entry")
            .long("entry")
            .short('n')
            .value_name("ENTRY_NAME")
            .help("List of entries to export.\nIf not specified, all entries will be exported. Wildcards are supported.")
            .num_args(1..)
            .required(false)
        )
        .arg(
            Arg::new("limit")
                .long("limit")
                .short('l')
                .help("The maximum number of records to export.\nIf not specified, all recordswill be exported.")
                .value_name("NUMBER")
                .value_parser(value_parser!(u64))
                .required(false)
        )
        .arg(
            Arg::new("ext")
                .long("ext")
                .short('x')
                .help("The file extension to use for the exported file.\nIf not specified, the default be guessed from the content type of the records.")
                .value_name("TEXT")
                .required(false)
        )
        .arg(
            Arg::new("with-meta")
                .long("with-meta")
                .short('m')
                .help("Export the metadata of the records along with the records in JSON format.\nIf not specified, only the records will be exported.")
                .required(false)
                .action(SetTrue)
        )
}

pub(crate) async fn cp_handler(ctx: &CliContext, args: &clap::ArgMatches) -> anyhow::Result<()> {
    let (source, src_res) = args
        .get_one::<(String, String)>("SOURCE_BUCKET_OR_FOLDER")
        .unwrap();
    let (destination, dst_res) = args
        .get_one::<(String, String)>("DESTINATION_BUCKET_OR_FOLDER")
        .unwrap();

    println!("{}, {}", source, destination);

    if src_res.is_empty() && dst_res.is_empty() {
        return Err(anyhow::anyhow!("Folder to folder copy is not supported."));
    } else if src_res.is_empty() {
        // Copy from filesystem to remote bucket
        return Err(anyhow::anyhow!("Folder to bucket copy is not supported."));
    } else if dst_res.is_empty() {
        // Copy from remote bucket to filesystem
        cp_bucket_to_folder(ctx, args).await?;
    } else {
        // Copy from remote bucket to remote bucket
        cp_bucket_to_bucket(ctx, args).await?;
    }
    Ok(())
}
