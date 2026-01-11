// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::io::IoConfig;
use crate::core::sync::{AsyncRwLock, RwLock};
use crate::core::weak::Weak;
use crate::storage::bucket::Bucket;
use crate::storage::entry::{Entry, RecordReader};
use crate::storage::query::base::QueryOptions;
use crate::storage::query::{next_query_id, QueryRx};
use log::debug;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::io::ReadRecord;
use reduct_base::msg::entry_api::QueryEntry;
use reduct_base::{no_content, not_found};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

const AGGREGATOR_BUFFER_SIZE: usize = 64;

#[allow(dead_code)]
pub(super) struct MultiEntryQuery {
    entry_queries: HashMap<String, u64>,
    aggregated_rx: Arc<AsyncRwLock<QueryRx>>,
    io_settings: IoConfig,
    options: QueryOptions,
    last_access: Instant,
}

impl Bucket {
    /// Initiate a query across multiple entries in the bucket.
    ///
    /// # Arguments
    ///
    /// * `request` - The query request containing the parameters for the query.
    ///
    /// # Returns
    /// A unique identifier for the initiated query.
    ///
    pub(crate) async fn query(&self, request: QueryEntry) -> Result<u64, ReductError> {
        let entries = self.filter_entries(&request).await?;
        let query_id = next_query_id();
        let options: QueryOptions = request.clone().into();

        let mut entry_queries = HashMap::new();
        for (entry_name, entry) in entries {
            let entry_query_id = entry.query(request.clone()).await?;
            entry_queries.insert(entry_name, entry_query_id);
        }

        let (aggregated_rx, io_settings) = self.init_aggregator(&entry_queries).await?;

        let multi_query = MultiEntryQuery {
            entry_queries,
            aggregated_rx,
            io_settings,
            options,
            last_access: Instant::now(),
        };

        self.queries.write().await?.insert(query_id, multi_query);

        Ok(query_id)
    }

    /// Get the query receiver for a given query ID.
    ///
    /// # Arguments
    ///
    /// * `query_id` - The unique identifier of the query.
    ///
    /// # Returns
    /// A weak reference to the query receiver and the IO configuration.
    ///
    pub(crate) async fn get_query_receiver(
        &self,
        query_id: u64,
    ) -> Result<(Weak<AsyncRwLock<QueryRx>>, IoConfig), ReductError> {
        Self::remove_expired_query(&self.queries, &self.name).await;

        let mut queries = self.queries.write().await?;
        let multi_query = queries.get_mut(&query_id).ok_or_else(|| {
            not_found!(
                "Query {} not found and it might have expired. Check TTL in your query request.",
                query_id
            )
        })?;

        multi_query.last_access = Instant::now();

        Ok((
            Weak::new(Arc::clone(&multi_query.aggregated_rx)),
            multi_query.io_settings.clone(),
        ))
    }

    async fn filter_entries(
        &self,
        request: &QueryEntry,
    ) -> Result<Vec<(String, Arc<Entry>)>, ReductError> {
        let entries = self.entries.read().await?;
        let requested_entries = match &request.entries {
            Some(entries) if entries.iter().any(|entry| entry == "*") => None,
            Some(entries) => Some(entries.clone()),
            None => None,
        };

        let matches_pattern = |entry: &str, patterns: &[String]| {
            patterns.iter().any(|pattern| {
                if let Some(prefix) = pattern.strip_suffix('*') {
                    entry.starts_with(prefix)
                } else {
                    entry == pattern
                }
            })
        };

        let results: Vec<(String, Arc<Entry>)> = entries
            .iter()
            .filter(|(name, _)| {
                requested_entries
                    .as_ref()
                    .map(|patterns| matches_pattern(name, patterns))
                    .unwrap_or(true)
            })
            .map(|(name, entry)| (name.clone(), Arc::clone(entry)))
            .collect();

        Ok(results)
    }

    async fn init_aggregator(
        &self,
        entry_queries: &HashMap<String, u64>,
    ) -> Result<(Arc<AsyncRwLock<QueryRx>>, IoConfig), ReductError> {
        let mut entry_receivers: HashMap<String, Arc<AsyncRwLock<QueryRx>>> = HashMap::new();
        let mut io_settings: Option<IoConfig> = None;

        for (entry_name, entry_query_id) in entry_queries {
            let entry = self.get_entry(entry_name).await?.upgrade()?;
            let (rx, settings) = entry.get_query_receiver(*entry_query_id).await?;
            let rx = rx.upgrade()?;

            if io_settings.is_none() {
                io_settings = Some(settings);
            }

            entry_receivers.insert(entry_name.clone(), rx);
        }

        let (tx, rx_out) = mpsc::channel(AGGREGATOR_BUFFER_SIZE);

        tokio::spawn(async move {
            Self::aggregate(entry_receivers, tx).await;
        });

        Ok((
            Arc::new(AsyncRwLock::new(rx_out)),
            io_settings.unwrap_or_default(),
        ))
    }

    async fn remove_expired_query(
        queries: &AsyncRwLock<HashMap<u64, MultiEntryQuery>>,
        bucket: &str,
    ) {
        if let Ok(mut queries) = queries.write().await {
            queries.retain(|id, handle| {
                if handle.last_access.elapsed() >= handle.options.ttl {
                    debug!("Query {}/{} expired", bucket, id);
                    return false;
                }
                true
            });
        }
    }

    async fn aggregate(
        entry_receivers: HashMap<String, Arc<AsyncRwLock<QueryRx>>>,
        tx: mpsc::Sender<Result<RecordReader, ReductError>>,
    ) {
        let mut pending_readers: HashMap<String, Option<RecordReader>> = HashMap::new();
        let mut completed: HashSet<String> = HashSet::new();

        loop {
            if tx.is_closed() {
                break;
            }

            let mut last_error: Option<ReductError> = None;
            let mut made_progress = false;

            for (entry_name, rx) in &entry_receivers {
                if matches!(
                    pending_readers.get(entry_name).and_then(|opt| opt.as_ref()),
                    Some(_)
                ) {
                    continue;
                }

                if completed.contains(entry_name) {
                    continue;
                }

                let recv_result = match rx.write().await {
                    Ok(mut guard) => guard.try_recv(),
                    Err(err) => {
                        last_error = Some(err);
                        break;
                    }
                };

                match recv_result {
                    Ok(Ok(reader)) => {
                        pending_readers.insert(entry_name.clone(), Some(reader));
                        made_progress = true;
                    }
                    Ok(Err(err)) => {
                        if err.status() != ErrorCode::NoContent {
                            last_error = Some(err);
                            break;
                        }
                        // NoContent just means no records right now; keep polling the entry
                        made_progress = true;
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                        completed.insert(entry_name.clone());
                        made_progress = true;
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                        // nothing ready for this entry right now
                    }
                }
            }

            if let Some(err) = last_error {
                let _ = tx.send(Err(err)).await;
                break;
            }

            if completed.len() == entry_receivers.len()
                && pending_readers.values().all(|v| v.is_none())
            {
                let _ = tx.send(Err(no_content!("No content"))).await;
                break;
            }

            let next = pending_readers
                .iter()
                .filter_map(|(name, reader)| {
                    reader
                        .as_ref()
                        .map(|r| (name.clone(), r.meta().timestamp()))
                })
                .min_by_key(|(_, ts)| *ts);

            if let Some((entry_name, _)) = next {
                if let Some(reader) = pending_readers
                    .get_mut(&entry_name)
                    .and_then(|opt| opt.take())
                {
                    if tx.send(Ok(reader)).await.is_err() {
                        break;
                    }
                }
            } else {
                // Nothing ready yet; yield to avoid busy loop while we wait for more data.
                if !made_progress {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::bucket::tests::{bucket, write};
    use reduct_base::error::ErrorCode;
    use reduct_base::io::ReadRecord;
    use reduct_base::msg::entry_api::{QueryEntry, QueryType};
    use reduct_base::not_found;
    use rstest::rstest;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::timeout;

    async fn collect_records(rx: Weak<AsyncRwLock<QueryRx>>) -> Vec<(String, u64)> {
        let rx = rx.upgrade().unwrap();
        let mut rx = rx.write().await.unwrap();
        let mut records = Vec::new();

        while let Some(result) = rx.recv().await {
            match result {
                Ok(reader) => {
                    let meta = reader.meta().clone();
                    records.push((meta.entry_name().to_string(), meta.timestamp()));
                }
                Err(err) => {
                    assert_eq!(err.status(), ErrorCode::NoContent);
                    break;
                }
            }
        }

        records
    }

    #[rstest]
    #[tokio::test]
    async fn aggregates_by_timestamp(bucket: Arc<Bucket>) {
        write(&bucket, "entry-a", 10, b"a1").await.unwrap();
        write(&bucket, "entry-b", 20, b"b1").await.unwrap();
        write(&bucket, "entry-b", 25, b"b2").await.unwrap();
        write(&bucket, "entry-a", 30, b"a2").await.unwrap();

        let query = QueryEntry {
            query_type: QueryType::Query,
            entries: Some(vec!["entry-a".into(), "entry-b".into()]),
            ..Default::default()
        };

        let id = bucket.query(query).unwrap();
        let (rx, _) = bucket.get_query_receiver(id).await.unwrap();

        let records = collect_records(rx).await;

        assert_eq!(
            records,
            vec![
                ("entry-a".to_string(), 10),
                ("entry-b".to_string(), 20),
                ("entry-b".to_string(), 25),
                ("entry-a".to_string(), 30),
            ]
        );
    }

    #[rstest]
    #[tokio::test]
    async fn filters_requested_entries(bucket: Arc<Bucket>) {
        write(&bucket, "entry-a", 10, b"a1").await.unwrap();
        write(&bucket, "entry-b", 20, b"b1").await.unwrap();
        write(&bucket, "entry-c", 15, b"c1").await.unwrap();

        let query = QueryEntry {
            query_type: QueryType::Query,
            entries: Some(vec!["entry-b".into(), "entry-c".into()]),
            ..Default::default()
        };

        let id = bucket.query(query).unwrap();
        let (rx, _) = bucket.get_query_receiver(id).await.unwrap();

        let records = collect_records(rx).await;

        assert_eq!(
            records,
            vec![("entry-c".to_string(), 15), ("entry-b".to_string(), 20),]
        );
    }

    #[rstest]
    #[tokio::test]
    async fn filters_by_prefix_wildcard(bucket: Arc<Bucket>) {
        write(&bucket, "acc-a", 10, b"a1").await.unwrap();
        write(&bucket, "acc-b", 20, b"b1").await.unwrap();
        write(&bucket, "other", 15, b"c1").await.unwrap();

        let query = QueryEntry {
            query_type: QueryType::Query,
            entries: Some(vec!["acc-*".into()]),
            ..Default::default()
        };

        let id = bucket.query(query).unwrap();
        let (rx, _) = bucket.get_query_receiver(id).await.unwrap();

        let records = collect_records(rx).await;

        assert_eq!(
            records,
            vec![("acc-a".to_string(), 10), ("acc-b".to_string(), 20)]
        );
    }

    #[rstest]
    #[tokio::test]
    async fn wildcard_all_entries(bucket: Arc<Bucket>) {
        write(&bucket, "entry-a", 10, b"a1").await.unwrap();
        write(&bucket, "entry-b", 20, b"b1").await.unwrap();

        let query = QueryEntry {
            query_type: QueryType::Query,
            entries: Some(vec!["*".into()]),
            ..Default::default()
        };

        let id = bucket.query(query).unwrap();
        let (rx, _) = bucket.get_query_receiver(id).await.unwrap();

        let records = collect_records(rx).await;

        assert_eq!(
            records,
            vec![("entry-a".to_string(), 10), ("entry-b".to_string(), 20)]
        );
    }

    #[rstest]
    #[tokio::test]
    async fn removes_expired_queries(bucket: Arc<Bucket>) {
        write(&bucket, "entry-a", 10, b"a1").await.unwrap();
        write(&bucket, "entry-b", 20, b"b1").await.unwrap();

        let query = QueryEntry {
            query_type: QueryType::Query,
            entries: Some(vec!["entry-a".into(), "entry-b".into()]),
            ttl: Some(1),
            ..Default::default()
        };

        let id = bucket.query(query).unwrap();
        let _ = bucket.get_query_receiver(id).await.unwrap();

        tokio::time::sleep(Duration::from_millis(1100)).await;

        let err = match bucket.get_query_receiver(id).await {
            Ok(_) => panic!("Expected query to expire"),
            Err(err) => err,
        };
        assert_eq!(
            err,
            not_found!(
                "Query {} not found and it might have expired. Check TTL in your query request.",
                id
            )
        );
    }

    #[tokio::test]
    async fn aggregate_stops_when_channel_closed() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);

        // Should exit immediately because the receiver is gone.
        assert!(timeout(
            Duration::from_millis(100),
            Bucket::aggregate(HashMap::new(), tx)
        )
        .await
        .is_ok());
    }
}
