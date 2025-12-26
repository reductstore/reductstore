// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::io::IoConfig;
use crate::core::sync::AsyncRwLock;
use crate::core::weak::Weak;
use crate::storage::bucket::Bucket;
use crate::storage::entry::{Entry, RecordReader};
use crate::storage::query::QueryRx;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::io::ReadRecord;
use reduct_base::msg::entry_api::QueryEntry;
use reduct_base::{no_content, not_found};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

const AGGREGATOR_BUFFER_SIZE: usize = 64;

#[allow(dead_code)]
pub(super) struct MultiEntryQuery {
    entry_queries: HashMap<String, u64>,
    aggregated_rx: Option<Arc<AsyncRwLock<QueryRx>>>,
    io_settings: Option<IoConfig>,
}

impl Bucket {
    #[allow(dead_code)]
    pub(crate) fn entry_query(&self, request: QueryEntry) -> Result<u64, ReductError> {
        static QUERY_ID: AtomicU64 = AtomicU64::new(1); // start with 1 because 0 may confuse with false

        let entries = self.filter_entries(&request)?;
        let query_id = QUERY_ID.fetch_add(1, Ordering::SeqCst);

        let entry_queries = entries
            .into_iter()
            .map(|(entry_name, entry)| {
                let entry_query_id = entry.query(request.clone())?;
                Ok((entry_name, entry_query_id))
            })
            .collect::<Result<HashMap<_, _>, ReductError>>()?;

        self.queries.write()?.insert(
            query_id,
            MultiEntryQuery {
                entry_queries,
                aggregated_rx: None,
                io_settings: None,
            },
        );

        Ok(query_id)
    }

    #[allow(dead_code)]
    pub(crate) async fn get_query_receiver(
        &self,
        query_id: u64,
    ) -> Result<(Weak<AsyncRwLock<QueryRx>>, IoConfig), ReductError> {
        let mut queries = self.queries.write()?;
        let multi_query = queries.get_mut(&query_id).ok_or_else(|| {
            not_found!(
                "Query {} not found and it might have expired. Check TTL in your query request.",
                query_id
            )
        })?;

        if multi_query.aggregated_rx.is_none() {
            self.init_aggregator(multi_query)?;
        }

        Ok((
            Weak::new(Arc::clone(multi_query.aggregated_rx.as_ref().unwrap())),
            multi_query.io_settings.clone().unwrap_or_default(),
        ))
    }

    #[allow(dead_code)]
    fn filter_entries(
        &self,
        request: &QueryEntry,
    ) -> Result<Vec<(String, Arc<Entry>)>, ReductError> {
        let entries = self.entries.read()?;
        let results: Vec<(String, Arc<Entry>)> = if let Some(requested_entries) = &request.entries {
            entries
                .iter()
                .filter(|(name, _)| requested_entries.contains(name))
                .map(|(name, entry)| (name.clone(), Arc::clone(entry)))
                .collect()
        } else {
            entries
                .iter()
                .map(|(name, entry)| (name.clone(), Arc::clone(entry)))
                .collect()
        };

        Ok(results)
    }

    fn init_aggregator(&self, multi_query: &mut MultiEntryQuery) -> Result<(), ReductError> {
        let mut entry_receivers: HashMap<String, Arc<AsyncRwLock<QueryRx>>> = HashMap::new();

        for (entry_name, entry_query_id) in &multi_query.entry_queries {
            let entry = self.get_entry(entry_name)?.upgrade()?;
            let (rx, settings) = entry.get_query_receiver(*entry_query_id)?;
            let rx = rx.upgrade()?;

            if multi_query.io_settings.is_none() {
                multi_query.io_settings = Some(settings);
            }

            entry_receivers.insert(entry_name.clone(), rx);
        }

        let (tx, rx_out) = mpsc::channel(AGGREGATOR_BUFFER_SIZE);

        tokio::spawn(async move {
            Self::aggregate(entry_receivers, tx).await;
        });

        multi_query.aggregated_rx = Some(Arc::new(AsyncRwLock::new(rx_out)));

        Ok(())
    }

    async fn aggregate(
        entry_receivers: HashMap<String, Arc<AsyncRwLock<QueryRx>>>,
        tx: mpsc::Sender<Result<RecordReader, ReductError>>,
    ) {
        let mut pending_readers: HashMap<String, Option<RecordReader>> = HashMap::new();

        loop {
            let mut last_error: Option<ReductError> = None;

            for (entry_name, rx) in &entry_receivers {
                if matches!(
                    pending_readers.get(entry_name).and_then(|opt| opt.as_ref()),
                    Some(_)
                ) {
                    continue;
                }

                let recv_result = match rx.write().await {
                    Ok(mut guard) => guard.recv().await,
                    Err(err) => {
                        last_error = Some(err);
                        break;
                    }
                };

                match recv_result {
                    Some(Ok(reader)) => {
                        pending_readers.insert(entry_name.clone(), Some(reader));
                    }
                    Some(Err(err)) => {
                        if err.status() != ErrorCode::NoContent {
                            last_error = Some(err);
                            break;
                        }
                    }
                    None => {}
                }
            }

            if let Some(err) = last_error {
                let _ = tx.send(Err(err)).await;
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
                let _ = tx.send(Err(no_content!("No content"))).await;
                break;
            }
        }
    }
}
