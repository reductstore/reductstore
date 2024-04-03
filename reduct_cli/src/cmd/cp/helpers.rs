// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::sync::Arc;
use std::time::Duration;

use crate::context::CliContext;
use crate::parse::widely_used_args::parse_label_args;
use bytesize::ByteSize;
use chrono::DateTime;
use clap::ArgMatches;
use futures_util::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reduct_rs::{Bucket, EntryInfo, ErrorCode, Labels, QueryBuilder, Record, ReductError};

use tokio::task::JoinSet;
use tokio::time::{sleep, Instant};

pub(super) struct QueryParams {
    start: Option<i64>,
    stop: Option<i64>,
    include_labels: Labels,
    exclude_labels: Labels,
    limit: Option<u64>,
    entries_filter: Vec<String>,
    parallel: usize,
    ttl: Duration,
}

impl Default for QueryParams {
    fn default() -> Self {
        Self {
            start: None,
            stop: None,
            include_labels: Labels::new(),
            exclude_labels: Labels::new(),
            limit: None,
            entries_filter: Vec::new(),
            parallel: 1,
            ttl: Duration::from_secs(60),
        }
    }
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

        let duration = self.history[0].1.elapsed().as_secs();
        self.speed =
            if self.speed_update.elapsed().as_secs() > 3 && self.history.len() > 10 && duration > 0
            {
                let result = self.history.iter().map(|(bytes, _)| bytes).sum::<u64>() / duration;
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

pub(super) fn parse_query_params(
    ctx: &CliContext,
    args: &ArgMatches,
) -> anyhow::Result<QueryParams> {
    let start = parse_time(args.get_one::<String>("start"))?;
    let stop = parse_time(args.get_one::<String>("stop"))?;
    let include_labels = parse_label_args(args.get_many::<String>("include"))?.unwrap_or_default();

    let exclude_labels = parse_label_args(args.get_many::<String>("exclude"))?.unwrap_or_default();

    let limit = args.get_one::<u64>("limit").map(|limit| *limit);

    let entries_filter = args
        .get_many::<String>("entries")
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
        parallel: ctx.parallel(),
        ttl: ctx.timeout() * ctx.parallel() as u32,
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

    query_builder = query_builder.include(query_params.include_labels.clone());
    query_builder = query_builder.exclude(query_params.exclude_labels.clone());

    if let Some(limit) = query_params.limit {
        query_builder = query_builder.limit(limit);
    }

    query_builder.ttl(query_params.ttl)
}

#[async_trait::async_trait]
pub(super) trait CopyVisitor {
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
    src_bucket: Bucket,
    query_params: QueryParams,
    visitor: V,
) -> anyhow::Result<()>
where
    V: CopyVisitor + Send + Sync + 'static,
{
    let entries = find_entries_to_copy(&src_bucket, &query_params).await?;

    let mut tasks = JoinSet::new();
    let progress = MultiProgress::new();
    let sty = ProgressStyle::with_template(
        "[{elapsed_precise}] {bar:40.cyan/blue} {percent_precise:6}% {msg}",
    )
    .unwrap()
    .progress_chars("##-");
    let semaphore = Arc::new(tokio::sync::Semaphore::new(query_params.parallel));

    let visitor = Arc::new(visitor);
    for entry in entries {
        let query_builder = build_query(&src_bucket, &entry, &query_params);
        let local_progress = ProgressBar::new(entry.latest_record - entry.oldest_record);
        let local_progress = progress.add(local_progress);
        let local_sem = Arc::clone(&semaphore);
        local_progress.set_style(sty.clone());

        let local_visitor = Arc::clone(&visitor);
        tasks.spawn(async move {
            let mut transfer_progress = TransferProgress::new(entry.clone());
            local_progress.set_message(transfer_progress.message());

            let _permit = local_sem.acquire().await.unwrap();

            let record_stream = query_builder.send().await?;
            tokio::pin!(record_stream);

            while let Some(record) = record_stream.next().await {
                let record = record?;
                local_progress.set_position(record.timestamp_us() - entry.oldest_record);

                let content_length = record.content_length() as u64;
                let result = local_visitor.visit(&entry.name, record).await;

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
        use crate::cmd::cp::cp_cmd;
        use crate::context::tests::context;

        use super::*;

        #[rstest]
        fn parse_start_stop(context: CliContext) {
            let args = cp_cmd()
                .try_get_matches_from(vec!["cp", "serv/buck1", "serv/buck2"])
                .unwrap();
            let query_params = parse_query_params(&context, &args).unwrap();

            assert_eq!(query_params.start, None);
            assert_eq!(query_params.stop, None);
        }

        #[rstest]
        fn parse_start_stop_with_values(context: CliContext) {
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
            let query_params = parse_query_params(&context, &args).unwrap();

            assert_eq!(query_params.start, Some(100));
            assert_eq!(query_params.stop, Some(200));
        }

        #[rstest]
        fn parse_start_stop_iso8601(context: CliContext) {
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
            let query_params = parse_query_params(&context, &args).unwrap();

            assert_eq!(query_params.start, Some(1672531200000000));
            assert_eq!(query_params.stop, Some(1672617600000000));
        }

        #[rstest]
        fn parse_include_exclude(context: CliContext) {
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
            let query_params = parse_query_params(&context, &args).unwrap();

            assert_eq!(
                query_params.include_labels,
                Labels::from_iter(vec![
                    ("key1".to_string(), "value1".to_string()),
                    ("key2".to_string(), "value2".to_string()),
                ])
            );
            assert_eq!(
                query_params.exclude_labels,
                Labels::from_iter(vec![
                    ("key3".to_string(), "value3".to_string()),
                    ("key4".to_string(), "value4".to_string()),
                ])
            );
        }

        #[rstest]
        fn parse_limit(context: CliContext) {
            let args = cp_cmd()
                .try_get_matches_from(vec!["cp", "serv/buck1", "serv/buck2", "--limit", "100"])
                .unwrap();
            let query_params = parse_query_params(&context, &args).unwrap();

            assert_eq!(query_params.limit, Some(100));
        }

        #[rstest]
        fn parse_entries_filter(context: CliContext) {
            let args = cp_cmd()
                .try_get_matches_from(vec![
                    "cp",
                    "serv/buck1",
                    "serv/buck2",
                    "--entries",
                    "entry-1",
                    "entry-2",
                ])
                .unwrap();
            let query_params = parse_query_params(&context, &args).unwrap();

            assert_eq!(
                query_params.entries_filter,
                vec![String::from("entry-1"), String::from("entry-2")]
            );
        }
    }

    mod downloading {
        use super::*;
        use crate::context::tests::{bucket, context};
        use crate::context::CliContext;
        use crate::io::reduct::build_client;
        use async_trait::async_trait;
        use bytes::Bytes;
        use mockall::mock;
        use mockall::predicate::{always, eq};

        mock! {
            pub Visitor {}
            #[async_trait]
            impl CopyVisitor for Visitor {
                async fn visit(&self, entry_name: &str, record: Record) -> Result<(), ReductError>;
            }
        }
        #[fixture]
        fn visitor() -> MockVisitor {
            MockVisitor::new()
        }

        #[fixture]
        async fn src_bucket(context: CliContext, #[future] bucket: String) -> Bucket {
            let client = build_client(&context, "local").await.unwrap();
            let bucket = client.create_bucket(&bucket.await).send().await.unwrap();
            bucket
                .write_record("entry-1")
                .data(Bytes::from_static(b"rec-1"))
                .send()
                .await
                .unwrap();
            bucket
                .write_record("entry-2")
                .data(Bytes::from_static(b"rec-2"))
                .send()
                .await
                .unwrap();

            bucket
        }

        #[rstest]
        #[tokio::test]
        async fn test_downloading(#[future] src_bucket: Bucket, mut visitor: MockVisitor) {
            let src_bucket = src_bucket.await;
            visitor.expect_visit().times(2).return_const(Ok(()));

            start_loading(src_bucket, QueryParams::default(), visitor)
                .await
                .unwrap();
        }

        #[rstest]
        #[tokio::test]
        async fn test_downloading_metadata(#[future] src_bucket: Bucket, mut visitor: MockVisitor) {
            let src_bucket = src_bucket.await;
            visitor
                .expect_visit()
                .withf(|entry, _record| entry == "entry-1")
                .times(1)
                .return_const(Ok(()));
            visitor
                .expect_visit()
                .withf(|entry, _record| entry == "entry-2")
                .times(1)
                .return_const(Ok(()));
            visitor
                .expect_visit()
                .withf(|entry, record| {
                    entry == "entry-3"
                        && record.timestamp_us() == 1
                        && record.labels().get("key").unwrap() == "value"
                        && record.content_type() == "text/plain"
                })
                .times(1)
                .return_const(Ok(()));

            src_bucket
                .write_record("entry-3")
                .timestamp_us(1)
                .add_label("key", "value")
                .content_type("text/plain")
                .data(Bytes::from_static(b"rec-3"))
                .send()
                .await
                .unwrap();

            start_loading(src_bucket, QueryParams::default(), visitor)
                .await
                .unwrap();
        }

        #[rstest]
        #[tokio::test]
        async fn test_downloading_finish_rest_if_one_fails(
            #[future] src_bucket: Bucket,
            mut visitor: MockVisitor,
        ) {
            let src_bucket = src_bucket.await;
            visitor
                .expect_visit()
                .withf(|entry, _record| entry == "entry-1")
                .times(1)
                .return_const(Err(ReductError::new(ErrorCode::Conflict, "Conflict")));
            visitor
                .expect_visit()
                .withf(|entry, _record| entry == "entry-2")
                .times(1)
                .return_const(Ok(()));

            let result = start_loading(src_bucket, QueryParams::default(), visitor).await;
            assert!(result.is_ok());
        }

        #[rstest]
        #[tokio::test]
        async fn test_downloading_entry_wildcard(
            #[future] src_bucket: Bucket,
            mut visitor: MockVisitor,
        ) {
            let src_bucket = src_bucket.await;
            visitor.expect_visit().times(2).return_const(Ok(()));

            let params = QueryParams {
                entries_filter: vec!["entry-*".to_string()],
                ..Default::default()
            };

            start_loading(src_bucket, params, visitor).await.unwrap();
        }

        #[rstest]
        #[tokio::test]
        async fn test_downloading_entry_filter(
            #[future] src_bucket: Bucket,
            mut visitor: MockVisitor,
        ) {
            let src_bucket = src_bucket.await;
            visitor
                .expect_visit()
                .times(1)
                .with(eq("entry-1"), always())
                .return_const(Ok(()));

            let params = QueryParams {
                entries_filter: vec!["entry-1".to_string()],
                ..Default::default()
            };

            start_loading(src_bucket, params, visitor).await.unwrap();
        }

        #[rstest]
        #[tokio::test]
        async fn test_downloading_limit(#[future] src_bucket: Bucket, mut visitor: MockVisitor) {
            let src_bucket = src_bucket.await;
            visitor.expect_visit().times(2).return_const(Ok(()));

            src_bucket
                .write_record("entry-1")
                .data(Bytes::from_static(b"rec-3"))
                .send()
                .await
                .unwrap();
            src_bucket
                .write_record("entry-2")
                .data(Bytes::from_static(b"rec-4"))
                .send()
                .await
                .unwrap();

            let params = QueryParams {
                limit: Some(1),
                ..Default::default()
            };

            start_loading(src_bucket, params, visitor).await.unwrap();
        }

        #[rstest]
        #[tokio::test]
        async fn test_downloading_include(#[future] src_bucket: Bucket, mut visitor: MockVisitor) {
            let src_bucket = src_bucket.await;
            visitor
                .expect_visit()
                .times(1)
                .with(eq("entry-1"), always())
                .return_const(Ok(()));

            src_bucket
                .write_record("entry-1")
                .data(Bytes::from_static(b"rec-3"))
                .add_label("key1", "value1")
                .send()
                .await
                .unwrap();

            let params = QueryParams {
                include_labels: Labels::from_iter(vec![("key1".to_string(), "value1".to_string())]),
                ..Default::default()
            };

            start_loading(src_bucket, params, visitor).await.unwrap();
        }

        #[rstest]
        #[tokio::test]
        async fn test_downloading_exclude(#[future] src_bucket: Bucket, mut visitor: MockVisitor) {
            let src_bucket = src_bucket.await;
            visitor
                .expect_visit()
                .times(1)
                .with(eq("entry-1"), always())
                .return_const(Ok(()));
            visitor
                .expect_visit()
                .times(1)
                .with(eq("entry-2"), always())
                .return_const(Ok(()));

            src_bucket
                .write_record("entry-1")
                .data(Bytes::from_static(b"rec-3"))
                .add_label("key1", "value1")
                .send()
                .await
                .unwrap();

            let params = QueryParams {
                exclude_labels: Labels::from_iter(vec![("key1".to_string(), "value1".to_string())]),
                ..Default::default()
            };

            start_loading(src_bucket, params, visitor).await.unwrap();
        }
    }
}
