// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::bucket::{create_update_bucket_args, parse_bucket_settings};

use crate::context::CliContext;
use crate::io::reduct::build_client;

use crate::io::std::output;
use clap::{ArgMatches, Command};
use reduct_rs::ReductClient;

pub(super) fn create_bucket_cmd() -> Command {
    let cmd = Command::new("create").about("Create a bucket");
    create_update_bucket_args(cmd)
}

pub(super) async fn create_bucket(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let (alias_or_url, bucket_name) = args.get_one::<(String, String)>("BUCKET_PATH").unwrap();
    let bucket_settings = parse_bucket_settings(args);

    let client: ReductClient = build_client(ctx, alias_or_url).await?;
    client
        .create_bucket(bucket_name)
        .settings(bucket_settings)
        .send()
        .await?;

    output!(ctx, "Bucket '{}' created", bucket_name);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::{bucket, context};
    use reduct_rs::QuotaType;
    use rstest::*;

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket(context: CliContext, #[future] bucket: String) {
        let args = create_bucket_cmd().get_matches_from(vec![
            "create",
            format!("local/{}", bucket.await).as_str(),
            "--quota-type",
            "FIFO",
            "--quota-size",
            "1GB",
            "--block-size",
            "32MB",
            "--block-records",
            "100",
        ]);

        assert_eq!(
            create_bucket(&context, &args).await.unwrap(),
            (),
            "Create bucket succeeded"
        );

        let client = build_client(&context, "local").await.unwrap();
        let bucket = client.get_bucket("test_bucket").await.unwrap();
        let settings = bucket.settings().await.unwrap();
        assert_eq!(settings.quota_type, Some(QuotaType::FIFO));
        assert_eq!(settings.quota_size, Some(1_000_000_000));
        assert_eq!(settings.max_block_size, Some(32_000_000));
        assert_eq!(settings.max_block_records, Some(100));

        assert_eq!(
            context.stdout().history(),
            vec!["Bucket 'test_bucket' created"]
        )
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket_invalid_path() {
        let args = create_bucket_cmd().try_get_matches_from(vec!["create", "local"]);
        assert_eq!(
            args.err().unwrap().to_string(),
            "error: invalid value 'local' for '<BUCKET_PATH>'\n\nFor more information, try '--help'.\n"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket_invalid_quota_type(#[future] bucket: String) {
        let args = create_bucket_cmd().try_get_matches_from(vec![
            "create",
            format!("local/{}", bucket.await).as_str(),
            "--quota-type",
            "INVALID",
        ]);
        assert_eq!(
            args.err().unwrap().to_string(),
            "error: invalid value 'INVALID' for '--quota-type <TEXT>'\n\nFor more information, try '--help'.\n",
            "Failed because of invalid quota type"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket_invalid_quota_size(_context: CliContext, #[future] bucket: String) {
        let args = create_bucket_cmd().try_get_matches_from(vec![
            "create",
            format!("local/{}", bucket.await).as_str(),
            "--quota-size",
            "INVALID",
        ]);
        assert_eq!(
            args.err().unwrap().to_string(),
            "error: invalid value 'INVALID' for '--quota-size <SIZE>'\n\nFor more information, try '--help'.\n",
            "Failed because of invalid quota size"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket_invalid_block_size(_context: CliContext, #[future] bucket: String) {
        let args = create_bucket_cmd().try_get_matches_from(vec![
            "create",
            format!("local/{}", bucket.await).as_str(),
            "--block-size",
            "INVALID",
        ]);
        assert_eq!(
            args.err().unwrap().to_string(),
            "error: invalid value 'INVALID' for '--block-size <SIZE>'\n\nFor more information, try '--help'.\n",
            "Failed because of invalid block size"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket_invalid_block_records(
        _context: CliContext,
        #[future] bucket: String,
    ) {
        let args = create_bucket_cmd().try_get_matches_from(vec![
            "create",
            format!("local/{}", bucket.await).as_str(),
            "--block-records",
            "INVALID",
        ]);
        assert_eq!(
            args.err().unwrap().to_string(),
            "error: invalid value 'INVALID' for '--block-records <NUMBER>': invalid digit found in string\n\nFor more information, try '--help'.\n",
            "Failed because of invalid block records"
        );
    }
}
