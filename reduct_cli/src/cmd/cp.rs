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
use crate::parse::widely_used_args::{make_entries_arg, make_exclude_arg, make_include_arg};
use crate::parse::ResourcePathParser;
use clap::ArgAction::SetTrue;
use clap::{value_parser, Arg, Command};

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
        .arg(make_include_arg())
        .arg(make_exclude_arg())
        .arg(make_entries_arg())
        .arg(
            Arg::new("limit")
                .long("limit")
                .short('l')
                .help("The maximum number of records to export.\nIf not specified, all records will be exported.")
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
    let (_, src_res) = args
        .get_one::<(String, String)>("SOURCE_BUCKET_OR_FOLDER")
        .unwrap();
    let (_, dst_res) = args
        .get_one::<(String, String)>("DESTINATION_BUCKET_OR_FOLDER")
        .unwrap();

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::context;
    use rstest::*;

    #[rstest]
    #[tokio::test]
    async fn folder_to_folder_unsupported(context: CliContext) {
        let args = cp_cmd()
            .try_get_matches_from(vec!["cp", "./", "./"])
            .unwrap();
        let result = cp_handler(&context, &args).await;
        assert!(result.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn folder_to_bucket_unsupported(context: CliContext) {
        let args = cp_cmd()
            .try_get_matches_from(vec!["cp", "./", "local/bucket"])
            .unwrap();
        let result = cp_handler(&context, &args).await;
        assert!(result.is_err());
    }
}
