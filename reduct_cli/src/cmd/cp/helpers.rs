// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use bytesize::ByteSize;
use chrono::DateTime;
use clap::ArgMatches;
use futures_util::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reduct_rs::{Bucket, EntryInfo, ErrorCode, QueryBuilder, Record, ReductError};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tokio::time::{sleep, Instant};

pub(super) struct QueryParams {
    start: Option<i64>,
    stop: Option<i64>,
    include_labels: Vec<String>,
    exclude_labels: Vec<String>,
    limit: Option<u64>,
    entries_filter: Vec<String>,
}

pub(super) struct TransferProgress {
    entry: EntryInfo,
    transferred_bytes: u64,
    speed: u64,
    history: Vec<(u64, Instant)>,
    speed_update: Instant,
}

impl TransferProgress {
    pub(super) fn new(entry: EntryInfo) -> Self {
        Self {
            entry,
            transferred_bytes: 0,
            speed: 0,
            history: Vec::new(),
            speed_update: Instant::now(),
        }
    }

    pub(super) fn update(&mut self, bytes: u64) {
        self.transferred_bytes += bytes;
        self.history.push((bytes, Instant::now()));

        self.speed = if self.speed_update.elapsed().as_secs() > 3 && self.history.len() > 10 {
            let result = self.history.iter().map(|(bytes, _)| bytes).sum::<u64>()
                / self.history[0].1.elapsed().as_secs();
            self.history.clear();
            self.speed_update = Instant::now();
            result
        } else {
            self.speed
        };
    }

    pub(crate) fn message(&self) -> String {
        format!(
            "Copying {} ({}, {}/s)",
            self.entry.name,
            ByteSize::b(self.transferred_bytes),
            ByteSize::b(self.speed)
        )
    }

    pub(crate) fn done(&self) -> String {
        format!(
            "Copied {} ({})",
            self.entry.name,
            ByteSize::b(self.transferred_bytes)
        )
    }
}

pub(super) fn parse_query_params(args: &ArgMatches) -> anyhow::Result<QueryParams> {
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

pub(super) async fn find_entries_to_copy(
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

pub(super) fn parse_time(time_str: Option<&String>) -> anyhow::Result<Option<i64>> {
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
        DateTime::parse_from_rfc3339(time_str)
            .map_err(|err| anyhow::anyhow!("Failed to parse time {}: {}", time_str, err))?
            .timestamp_micros()
    };

    Ok(Some(time))
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

#[async_trait::async_trait]
pub(super) trait Visitor {
    async fn visit(&self, entry_name: &str, record: Record) -> Result<(), ReductError>;
}

/**
 * Start loading records from the source bucket and move to a visitor
 *
 * # Arguments
 *
 * `src_bucket` - The source bucket
 * `query_params` - The query parameters. Use `parse_query_params` to parse the arguments
 * `visitor` - The visitor that will receive the records
 */
pub(super) async fn start_loading<V>(
    src_bucket: &Bucket,
    query_params: &QueryParams,
    visitor: Arc<RwLock<V>>,
) -> anyhow::Result<()>
where
    V: Visitor + Send + Sync + 'static,
{
    let entries = find_entries_to_copy(&src_bucket, &query_params).await?;

    let mut tasks = JoinSet::new();
    let progress = MultiProgress::new();
    let sty = ProgressStyle::with_template(
        "[{elapsed_precise}] {bar:40.cyan/blue} {percent_precise:6}% {msg}",
    )
    .unwrap()
    .progress_chars("##-");

    for entry in entries {
        let query_builder = build_query(&src_bucket, &entry, &query_params);
        let local_progress = ProgressBar::new(entry.latest_record - entry.oldest_record);
        let local_progress = progress.add(local_progress);
        local_progress.set_style(sty.clone());

        let local_visitor = Arc::clone(&visitor);

        tasks.spawn(async move {
            let mut transfer_progress = TransferProgress::new(entry.clone());
            local_progress.set_message(transfer_progress.message());

            let record_stream = query_builder.send().await?;
            tokio::pin!(record_stream);

            while let Some(record) = record_stream.next().await {
                let record = record?;
                local_progress.set_position(record.timestamp_us() - entry.oldest_record);

                let content_length = record.content_length() as u64;
                let result = local_visitor.read().await.visit(&entry.name, record).await;

                if let Err(err) = result {
                    // ignore conflict errors
                    if err.status() != ErrorCode::Conflict {
                        local_progress.set_message(err.to_string());
                        local_progress.abandon();
                        return Err(err);
                    }
                }

                transfer_progress.update(content_length);
                local_progress.set_message(transfer_progress.message());

                sleep(Duration::from_micros(5)).await;
            }

            local_progress.set_message(transfer_progress.done());
            local_progress.abandon();
            Ok(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        let _ = result?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    mod parse_query_params {
        use super::*;
        use crate::cmd::cp::cp_cmd;

        #[rstest]
        fn parse_start_stop() {
            let args = cp_cmd()
                .try_get_matches_from(vec!["cp", "serv/buck1", "serv/buck2"])
                .unwrap();
            let query_params = parse_query_params(&args).unwrap();

            assert_eq!(query_params.start, None);
            assert_eq!(query_params.stop, None);
        }

        #[rstest]
        fn parse_start_stop_with_values() {
            let args = cp_cmd()
                .try_get_matches_from(vec![
                    "cp",
                    "serv/buck1",
                    "serv/buck2",
                    "--start",
                    "100",
                    "--stop",
                    "200",
                ])
                .unwrap();
            let query_params = parse_query_params(&args).unwrap();

            assert_eq!(query_params.start, Some(100));
            assert_eq!(query_params.stop, Some(200));
        }

        #[rstest]
        fn parse_start_stop_iso8601() {
            let args = cp_cmd()
                .try_get_matches_from(vec![
                    "cp",
                    "serv/buck1",
                    "serv/buck2",
                    "--start",
                    "2023-01-01T00:00:00Z",
                    "--stop",
                    "2023-01-02T00:00:00Z",
                ])
                .unwrap();
            let query_params = parse_query_params(&args).unwrap();

            assert_eq!(query_params.start, Some(1672531200000000));
            assert_eq!(query_params.stop, Some(1672617600000000));
        }

        #[rstest]
        fn parse_include_exclude() {
            let args = cp_cmd()
                .try_get_matches_from(vec![
                    "cp",
                    "serv/buck1",
                    "serv/buck2",
                    "--include",
                    "key1=value1",
                    "key2=value2",
                    "--exclude",
                    "key3=value3",
                    "key4=value4",
                ])
                .unwrap();
            let query_params = parse_query_params(&args).unwrap();

            assert_eq!(
                query_params.include_labels,
                vec!["key1=value1", "key2=value2"]
            );
            assert_eq!(
                query_params.exclude_labels,
                vec!["key3=value3", "key4=value4"]
            );
        }

        #[rstest]
        fn parse_limit() {
            let args = cp_cmd()
                .try_get_matches_from(vec!["cp", "serv/buck1", "serv/buck2", "--limit", "100"])
                .unwrap();
            let query_params = parse_query_params(&args).unwrap();

            assert_eq!(query_params.limit, Some(100));
        }

        #[rstest]
        fn parse_entries_filter() {
            let args = cp_cmd()
                .try_get_matches_from(vec![
                    "cp",
                    "serv/buck1",
                    "serv/buck2",
                    "--entry",
                    "entry-1",
                    "entry-2",
                ])
                .unwrap();
            let query_params = parse_query_params(&args).unwrap();

            assert_eq!(
                query_params.entries_filter,
                vec![String::from("entry-1"), String::from("entry-2")]
            );
        }
    }
}
