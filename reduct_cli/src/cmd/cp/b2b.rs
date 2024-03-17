// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::cp::helpers::{parse_query_params, start_loading, Visitor};
use crate::context::CliContext;
use crate::io::reduct::build_client;
use clap::ArgMatches;
use futures_util::StreamExt;
use reduct_rs::{Bucket, ErrorCode, Record, ReductError};
use std::sync::Arc;
use tokio::sync::RwLock;

pub(crate) async fn cp_bucket_to_bucket(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let (src_instance, src_bucket) = args
        .get_one::<(String, String)>("SOURCE_BUCKET_OR_FOLDER")
        .map(|(src_instance, src_bucket)| (src_instance.clone(), src_bucket.clone()))
        .unwrap();
    let (dst_instance, dst_bucket) = args
        .get_one::<(String, String)>("DESTINATION_BUCKET_OR_FOLDER")
        .map(|(dst_instance, dst_bucket)| (dst_instance.clone(), dst_bucket.clone()))
        .unwrap();

    let query_params = parse_query_params(&args)?;

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

    let dst_bucket = Arc::new(RwLock::new(dst_bucket));

    struct CopyVisitor {
        dst_bucket: Arc<RwLock<Bucket>>,
    }

    #[async_trait::async_trait]
    impl Visitor for CopyVisitor {
        async fn visit(&self, entry_name: &str, record: Record) -> Result<(), ReductError> {
            self.dst_bucket
                .write()
                .await
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

    let visitor = Arc::new(RwLock::new(CopyVisitor {
        dst_bucket: dst_bucket.clone(),
    }));

    start_loading(&src_bucket, &query_params, visitor).await
}
