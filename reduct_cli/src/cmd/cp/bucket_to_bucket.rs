// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::context::CliContext;
use crate::io::reduct::build_client;
use chrono::DateTime;
use clap::ArgMatches;
use futures_util::StreamExt;
use reduct_rs::{EntryInfo, ErrorCode};

pub(crate) async fn cp_bucket_to_bucket(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let (src_instance, src_bucket) = args
        .get_one::<(String, String)>("SOURCE_BUCKET_OR_FOLDER")
        .unwrap();
    let (dst_instance, dst_bucket) = args
        .get_one::<(String, String)>("DESTINATION_BUCKET_OR_FOLDER")
        .unwrap();

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

    let limit = args.get_one::<u64>("limit");

    let entries_filter = args
        .get_many::<String>("entries")
        .unwrap_or_default()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

    let src_bucket = build_client(ctx, src_instance)
        .await?
        .get_bucket(src_bucket)
        .await?;

    let dst_client = build_client(ctx, dst_instance).await?;
    let dst_bucket = match dst_client.get_bucket(dst_bucket).await {
        Ok(bucket) => bucket,
        Err(err) => {
            if err.status() == ErrorCode::NotFound {
                // Create the bucket if it does not exist with the same settings as the source bucket
                dst_client
                    .create_bucket(dst_bucket)
                    .settings(src_bucket.settings().await?)
                    .send()
                    .await?
            } else {
                return Err(err.into());
            }
        }
    };

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

    for entry in entries {
        let mut query_builder = src_bucket.query(&entry.name);
        if let Some(start) = start {
            query_builder = query_builder.start_us(start as u64);
        }
        if let Some(stop) = stop {
            query_builder = query_builder.stop_us(stop as u64);
        }
        if !include_labels.is_empty() {
            for label in &include_labels {
                let mut label = label.splitn(2, '=');
                let key = label.next().unwrap();
                let value = label.next().unwrap();
                query_builder = query_builder.add_include(key, value);
            }
        }

        if !exclude_labels.is_empty() {
            for label in &exclude_labels {
                let mut label = label.splitn(2, '=');
                let key = label.next().unwrap();
                let value = label.next().unwrap();
                query_builder = query_builder.add_exclude(key, value);
            }
        }

        if let Some(limit) = limit {
            query_builder = query_builder.limit(*limit);
        }

        let record_stream = query_builder.send().await?;
        tokio::pin!(record_stream);
        while let Some(record) = record_stream.next().await {
            let record = record?;

            let result = dst_bucket
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
                    return Err(err.into());
                }
            }

            println!("Record copied from {} to {}", src_instance, dst_instance);
        }
    }

    Ok(())
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
