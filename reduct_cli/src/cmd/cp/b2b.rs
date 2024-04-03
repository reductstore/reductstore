// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::cp::helpers::{parse_query_params, start_loading, CopyVisitor};
use crate::context::CliContext;
use crate::io::reduct::build_client;
use clap::ArgMatches;
use reduct_rs::{Bucket, ErrorCode, Record, ReductError};
use std::sync::Arc;

struct CopyToBucketVisitor {
    dst_bucket: Arc<Bucket>,
}

#[async_trait::async_trait]
impl CopyVisitor for CopyToBucketVisitor {
    async fn visit(&self, entry_name: &str, record: Record) -> Result<(), ReductError> {
        self.dst_bucket
            .write_record(entry_name)
            .timestamp_us(record.timestamp_us())
            .labels(record.labels().clone())
            .content_type(record.content_type())
            .content_length(record.content_length() as u64)
            .stream(record.stream_bytes())
            .send()
            .await
    }
}

pub(crate) async fn cp_bucket_to_bucket(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let (src_instance, src_bucket) = args
        .get_one::<(String, String)>("SOURCE_BUCKET_OR_FOLDER")
        .map(|(src_instance, src_bucket)| (src_instance.clone(), src_bucket.clone()))
        .unwrap();
    let (dst_instance, dst_bucket) = args
        .get_one::<(String, String)>("DESTINATION_BUCKET_OR_FOLDER")
        .map(|(dst_instance, dst_bucket)| (dst_instance.clone(), dst_bucket.clone()))
        .unwrap();

    let query_params = parse_query_params(ctx, &args)?;

    let src_bucket = build_client(ctx, &src_instance)
        .await?
        .get_bucket(&src_bucket)
        .await?;

    let dst_client = build_client(ctx, &dst_instance).await?;
    let dst_bucket = match dst_client.get_bucket(&dst_bucket).await {
        Ok(bucket) => bucket,
        Err(err) => {
            if err.status() == ErrorCode::NotFound {
                // Create the bucket if it does not exist with the same settings as the source bucket
                dst_client
                    .create_bucket(&dst_bucket)
                    .settings(src_bucket.settings().await?)
                    .send()
                    .await?
            } else {
                return Err(err.into());
            }
        }
    };

    let visitor = CopyToBucketVisitor {
        dst_bucket: Arc::new(dst_bucket),
    };

    start_loading(src_bucket, query_params, visitor).await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::cmd::cp::cp_cmd;
    use crate::context::tests::{bucket, bucket2, context};
    use bytes::Bytes;
    use reduct_rs::{QuotaType, RecordBuilder};
    use rstest::{fixture, rstest};

    use super::*;

    mod copy_visitor {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_visit(context: CliContext, #[future] bucket: String, record: Record) {
            let client = build_client(&context, "local").await.unwrap();
            let dst_bucket = client
                .create_bucket(&bucket.await)
                .exist_ok(true)
                .send()
                .await
                .unwrap();

            let dst_bucket = Arc::new(dst_bucket);
            let visitor = CopyToBucketVisitor {
                dst_bucket: Arc::clone(&dst_bucket),
            };

            // should write the record to the destination bucket
            visitor.visit("test", record).await.unwrap();

            let record = dst_bucket
                .read_record("test")
                .timestamp_us(123456)
                .send()
                .await
                .unwrap();
            assert_eq!(record.content_type(), "text/plain");
            assert_eq!(record.content_length(), 4);
            assert_eq!(record.bytes().await.unwrap(), Bytes::from_static(b"test"));
        }
    }

    mod cp_bucket_to_bucket {
        use super::*;
        use crate::cmd::cp::cp_cmd;

        #[rstest]
        #[tokio::test]
        async fn test_cp_bucket_to_bucket(
            context: CliContext,
            #[future] bucket: String,
            #[future] bucket2: String,
            record: Record,
        ) {
            let client = build_client(&context, "local").await.unwrap();
            let src_bucket = client.create_bucket(&bucket.await).send().await.unwrap();
            let dst_bucket = client.create_bucket(&bucket2.await).send().await.unwrap();

            src_bucket
                .write_record("test")
                .timestamp_us(123456)
                .data(record.bytes().await.unwrap())
                .send()
                .await
                .unwrap();

            let args = cp_cmd()
                .try_get_matches_from(vec![
                    "cp",
                    format!("local/{}", src_bucket.name()).as_str(),
                    format!("local/{}", dst_bucket.name()).as_str(),
                ])
                .unwrap();

            cp_bucket_to_bucket(&context, &args).await.unwrap();

            let record = dst_bucket
                .read_record("test")
                .timestamp_us(123456)
                .send()
                .await
                .unwrap();
            assert_eq!(record.bytes().await.unwrap(), Bytes::from_static(b"test"));
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_cp_bucket_to_create_dst_bucket(
        context: CliContext,
        #[future] bucket: String,
        #[future] bucket2: String,
    ) {
        let dst_bucket = bucket2.await;
        let client = build_client(&context, "local").await.unwrap();
        let src_bucket = client
            .create_bucket(&bucket.await)
            .quota_type(QuotaType::FIFO)
            .send()
            .await
            .unwrap();

        let args = cp_cmd()
            .try_get_matches_from(vec![
                "cp",
                format!("local/{}", src_bucket.name()).as_str(),
                format!("local/{}", dst_bucket).as_str(),
            ])
            .unwrap();

        cp_bucket_to_bucket(&context, &args).await.unwrap();

        let dst_bucket = client.get_bucket("test_bucket").await.unwrap();
        assert_eq!(
            dst_bucket.settings().await.unwrap().quota_type,
            Some(QuotaType::FIFO),
            "The destination bucket should have the same settings as the source bucket"
        );
    }

    #[fixture]
    fn record() -> Record {
        RecordBuilder::new()
            .timestamp_us(123456)
            .content_type("text/plain".to_string())
            .add_label("key".to_string(), "value".to_string())
            .data(Bytes::from_static(b"test"))
            .build()
    }
}
