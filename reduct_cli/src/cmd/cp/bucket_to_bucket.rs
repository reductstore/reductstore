use std::ptr::write;
// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::context::CliContext;
use crate::io::reduct::build_client;
use chrono::DateTime;
use clap::ArgMatches;
use futures_util::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reduct_rs::{Bucket, EntryInfo, ErrorCode, QueryBuilder, ReductError};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::{JoinHandle, JoinSet};

struct QueryParams {
    start: Option<i64>,
    stop: Option<i64>,
    include_labels: Vec<String>,
    exclude_labels: Vec<String>,
    limit: Option<u64>,
    entries_filter: Vec<String>,
}

fn parse_query_params(args: &ArgMatches) -> anyhow::Result<QueryParams> {
    let start = parse_time(args.get_one::<String>("start"))?;
    let stop = parse_time(args.get_one::<String>("stop"))?;
    let include_labels = args
        .get_many::<String>("include")
        .unwrap_or_default()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

    let exclude_labels = args
        .get_many::<String>("exclude")
        .unwrap_or_default()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

    let limit = args.get_one::<u64>("limit").map(|limit| *limit);

    let entries_filter = args
        .get_many::<String>("entry")
        .unwrap_or_default()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

    Ok(QueryParams {
        start,
        stop,
        include_labels,
        exclude_labels,
        limit,
        entries_filter,
    })
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

    let entries = find_entries_to_copy(&src_bucket, &query_params).await?;

    let mut tasks: JoinSet<Result<(), ReductError>> = JoinSet::new();
    let dst_bucket = Arc::new(RwLock::new(dst_bucket));

    let progress = MultiProgress::new();
    let sty = ProgressStyle::with_template(
        "[{elapsed_precise}] {bar:40.cyan/blue} {percent_precise:6}% {msg}",
    )
    .unwrap()
    .progress_chars("##-");

    for entry in entries {
        let query_builder = build_query(&src_bucket, &entry, &query_params);

        let local_dst_bucket = Arc::clone(&dst_bucket);
        let local_progress = ProgressBar::new(entry.latest_record as u64);
        let local_progress = progress.add(local_progress);
        local_progress.set_style(sty.clone());

        tasks.spawn(async move {
            local_progress.set_message(format!("Copying {}", entry.name));
            let record_stream = query_builder.send().await?;
            tokio::pin!(record_stream);

            while let Some(record) = record_stream.next().await {
                let record = record?;
                local_progress.set_position(record.timestamp_us() as u64);

                let timestamp = record.timestamp_us();
                let result = local_dst_bucket
                    .write()
                    .await
                    .write_record(&entry.name)
                    .timestamp_us(record.timestamp_us())
                    .labels(record.labels().clone())
                    .content_type(record.content_type())
                    .content_length(record.content_length() as u64)
                    .stream(record.stream_bytes())
                    .send()
                    .await;
                if let Err(err) = result {
                    // ignore conflict errors

                    if err.status() != ErrorCode::Conflict {
                        local_progress.set_message(err.to_string());
                        local_progress.abandon();
                        return Err(err);
                    }
                }
            }

            Ok(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        result?;
    }

    Ok(())
}

fn build_query(src_bucket: &Bucket, entry: &EntryInfo, query_params: &QueryParams) -> QueryBuilder {
    let mut query_builder = src_bucket.query(&entry.name);
    if let Some(start) = query_params.start {
        query_builder = query_builder.start_us(start as u64);
    }
    if let Some(stop) = query_params.stop {
        query_builder = query_builder.stop_us(stop as u64);
    }
    if !query_params.include_labels.is_empty() {
        for label in &query_params.include_labels {
            let mut label = label.splitn(2, '=');
            let key = label.next().unwrap();
            let value = label.next().unwrap();
            query_builder = query_builder.add_include(key, value);
        }
    }

    if !query_params.exclude_labels.is_empty() {
        for label in &query_params.exclude_labels {
            let mut label = label.splitn(2, '=');
            let key = label.next().unwrap();
            let value = label.next().unwrap();
            query_builder = query_builder.add_exclude(key, value);
        }
    }

    if let Some(limit) = query_params.limit {
        query_builder = query_builder.limit(limit);
    }
    query_builder
}

async fn find_entries_to_copy(
    src_bucket: &Bucket,
    query_params: &QueryParams,
) -> anyhow::Result<Vec<EntryInfo>> {
    let entries_filter = &query_params.entries_filter;
    let entries = src_bucket
        .entries()
        .await?
        .iter()
        .filter(|entry| -> bool {
            if entries_filter.is_empty() || entries_filter.contains(&entry.name) {
                true
            } else {
                // check wildcard
                entries_filter.iter().any(|filter| {
                    filter.ends_with('*') && entry.name.starts_with(&filter[..filter.len() - 1])
                })
            }
        })
        .map(|entry| entry.clone())
        .collect::<Vec<EntryInfo>>();
    Ok(entries)
}

fn parse_time(time_str: Option<&String>) -> anyhow::Result<Option<i64>> {
    if time_str.is_none() {
        return Ok(None);
    }

    let time_str = time_str.unwrap();
    let time = if let Ok(time) = time_str.parse::<i64>() {
        if time < 0 {
            return Err(anyhow::anyhow!("Time must be a positive integer"));
        }
        time
    } else {
        // try parse as ISO 8601
        DateTime::parse_from_rfc3339(time_str)?.timestamp_micros()
    };

    Ok(Some(time))
}
