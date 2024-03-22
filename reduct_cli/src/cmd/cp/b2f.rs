// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cmd::cp::helpers::{parse_query_params, start_loading, Visitor};
use crate::context::CliContext;
use crate::io::reduct::build_client;
use clap::ArgMatches;
use futures_util::StreamExt;
use mime_guess::{get_extensions, get_mime_extensions, get_mime_extensions_str};
use reduct_rs::{Bucket, ErrorCode, Labels, Record, ReductError};
use serde::Serialize;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use tokio::{fs, pin};

struct CopyToFolderVisitor {
    dst_folder: PathBuf,
    ext: Option<String>,
    with_meta: bool,
}

#[derive(Serialize)]
struct Meta {
    timestamp: u64,
    labels: Labels,
    content_type: String,
    content_length: usize,
}

#[async_trait::async_trait]
impl Visitor for CopyToFolderVisitor {
    async fn visit(&self, entry_name: &str, record: Record) -> Result<(), ReductError> {
        fs::create_dir_all(&self.dst_folder.join(entry_name)).await?;

        let ext = if let Some(ext) = &self.ext {
            ext.clone()
        } else {
            if let Some((top, sub)) = record.content_type().split_once('/') {
                if let Some(ext) = get_extensions(top, sub) {
                    ext.last().unwrap_or(&"bin").to_string()
                } else {
                    "bin".to_string()
                }
            } else {
                "bin".to_string()
            }
        };

        let file_path =
            self.dst_folder
                .join(entry_name)
                .join(format!("{}.{}", record.timestamp_us(), ext));
        let mut file = fs::File::create(file_path).await?;

        let meta = Meta {
            timestamp: record.timestamp_us(),
            labels: record.labels().clone(),
            content_type: record.content_type().to_string(),
            content_length: record.content_length(),
        };

        let stream = record.stream_bytes();
        pin!(stream);
        let mut count = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
            count += chunk.len();
        }

        if count != meta.content_length {
            return Err(ReductError::new(
                ErrorCode::Unknown,
                "Content length mismatch",
            ));
        }

        if self.with_meta {
            let meta_path = self
                .dst_folder
                .join(entry_name)
                .join(format!("{}-meta.json", meta.timestamp));
            let mut meta_file = fs::File::create(meta_path).await?;
            let meta = serde_json::to_string_pretty(&meta)
                .map_err(|err| ReductError::new(ErrorCode::Unknown, &err.to_string()))?;
            meta_file.write_all(meta.as_bytes()).await?;
        }

        Ok(())
    }
}

pub(crate) async fn cp_bucket_to_folder(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let (src_instance, src_bucket) = args
        .get_one::<(String, String)>("SOURCE_BUCKET_OR_FOLDER")
        .map(|(src_instance, src_bucket)| (src_instance.clone(), src_bucket.clone()))
        .unwrap();
    let (first_folder, rest_path) = args
        .get_one::<(String, String)>("DESTINATION_BUCKET_OR_FOLDER")
        .map(|(dst_instance, dst_bucket)| (dst_instance.clone(), dst_bucket.clone()))
        .unwrap();

    let query_params = parse_query_params(ctx, &args)?;
    let src_bucket = build_client(ctx, &src_instance)
        .await?
        .get_bucket(&src_bucket)
        .await?;

    let dst_folder = PathBuf::from(first_folder).join(rest_path);
    let visitor = CopyToFolderVisitor {
        dst_folder,
        ext: args.get_one::<String>("ext").map(|ext| ext.to_string()),
        with_meta: args.get_one::<bool>("with-meta").unwrap().clone(),
    };

    let visitor = Arc::new(RwLock::new(visitor));
    start_loading(&src_bucket, &query_params, visitor).await?;

    Ok(())
}
