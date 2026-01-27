// Copyright 2023-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod entry_loader;
pub(crate) mod io;
mod read_record;
mod remove_records;
pub(crate) mod update_labels;
mod write_record;

use crate::cfg::io::IoConfig;
use crate::cfg::Cfg;
use crate::core::sync::AsyncRwLock;
use crate::core::weak::Weak;
use crate::storage::block_manager::block_index::BlockIndex;
use crate::storage::block_manager::{BlockManager, BLOCK_INDEX_FILE};
use crate::storage::entry::entry_loader::EntryLoader;
use crate::storage::proto::ts_to_us;
use crate::storage::query::base::QueryOptions;
use crate::storage::query::{build_query, next_query_id, spawn_query_task, QueryRx};
pub(crate) use io::record_reader::RecordReader;
pub(crate) use io::record_writer::{RecordDrainer, RecordWriter};
use log::{debug, error};
use reduct_base::error::ReductError;
use reduct_base::msg::entry_api::{EntryInfo, QueryEntry};
use reduct_base::msg::status::ResourceStatus;
use reduct_base::{conflict, internal_server_error, not_found};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinHandle;

struct QueryHandle {
    rx: Arc<AsyncRwLock<QueryRx>>,
    options: QueryOptions,
    last_access: Instant,
    #[allow(dead_code)]
    query_task_handle: JoinHandle<()>,
    io_settings: IoConfig,
}

type QueryHandleMap = HashMap<u64, QueryHandle>;
type QueryHandleMapRef = Arc<AsyncRwLock<QueryHandleMap>>;

/// Entry is a time series in a bucket.
pub(crate) struct Entry {
    name: String,
    bucket_name: String,
    settings: AsyncRwLock<EntrySettings>,
    block_manager: Arc<AsyncRwLock<BlockManager>>,
    queries: QueryHandleMapRef,
    status: AsyncRwLock<ResourceStatus>,
    path: PathBuf,
    cfg: Arc<Cfg>,
}

#[derive(PartialEq)]
enum RecordType {
    Latest,
    Belated,
    BelatedFirst,
}

/// EntryOptions is the options for creating a new entry.
#[derive(PartialEq, Debug, Clone)]
pub struct EntrySettings {
    pub max_block_size: u64,
    pub max_block_records: u64,
}

impl Entry {
    pub async fn try_build(
        name: &str,
        path: PathBuf,
        settings: EntrySettings,
        cfg: Arc<Cfg>,
    ) -> Result<Self, ReductError> {
        let path = path.join(name);
        Ok(Self {
            name: name.to_string(),
            bucket_name: path
                .parent()
                .unwrap()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string(),
            settings: AsyncRwLock::new(settings),
            block_manager: Arc::new(AsyncRwLock::new(
                BlockManager::build(
                    path.clone(),
                    BlockIndex::new(path.join(BLOCK_INDEX_FILE)),
                    cfg.clone(),
                )
                .await,
            )),
            queries: Arc::new(AsyncRwLock::new(HashMap::new())),
            status: AsyncRwLock::new(ResourceStatus::Ready),
            path,
            cfg,
        })
    }

    pub(crate) async fn restore(
        path: PathBuf,
        options: EntrySettings,
        cfg: Arc<Cfg>,
    ) -> Result<Option<Entry>, ReductError> {
        let entry = EntryLoader::restore_entry(path, options, cfg).await?;
        Ok(entry)
    }

    /// Query records for a time range.
    ///
    /// # Arguments
    ///
    /// * `query_parameters` - The query parameters.
    ///
    /// # Returns
    ///
    /// * `u64` - The query ID.
    /// * `HTTPError` - The error if any.
    pub async fn query(&self, query_parameters: QueryEntry) -> Result<u64, ReductError> {
        let (start, stop) = self.get_query_time_range(&query_parameters).await?;
        let id = next_query_id();
        let block_manager = Arc::clone(&self.block_manager);

        let options: QueryOptions = query_parameters.into();
        let query = build_query(
            self.name.clone(),
            start,
            stop,
            options.clone(),
            self.cfg.io_conf.clone(),
        )?;

        let io_settings = query.as_ref().io_settings().clone();
        let (rx, task_handle) =
            spawn_query_task(id, self.task_group(), query, options.clone(), block_manager);

        self.queries.write().await?.insert(
            id,
            QueryHandle {
                rx: Arc::new(AsyncRwLock::new(rx)),
                options,
                last_access: Instant::now(),
                query_task_handle: task_handle,
                io_settings,
            },
        );

        Ok(id)
    }

    /// Returns the next record for a query.
    ///
    /// # Arguments
    ///
    /// * `query_id` - The query ID.
    ///
    /// # Returns
    ///
    /// * `(RecordReader, IoConfig)` - The record reader to read the record content in chunks and a boolean indicating if the query is done.
    /// * `HTTPError` - The error if any.
    pub async fn get_query_receiver(
        &self,
        query_id: u64,
    ) -> Result<(Weak<AsyncRwLock<QueryRx>>, IoConfig), ReductError> {
        let entry_path = format!("{}/{}", self.bucket_name, self.name);
        let queries = Arc::clone(&self.queries);
        Self::remove_expired_query(queries, entry_path).await?;

        let mut queries = self.queries.write().await?;
        let query = queries.get_mut(&query_id).ok_or_else(|| {
            not_found!(
                "Query {} not found and it might have expired. Check TTL in your query request.",
                query_id
            )
        })?;

        query.last_access = Instant::now();
        Ok((Weak::new(Arc::clone(&query.rx)), query.io_settings.clone()))
    }

    /// Returns stats about the entry.
    pub async fn info(&self) -> Result<EntryInfo, ReductError> {
        let name = self.name.clone();
        let status_result = self.status();

        let mut bm = self.block_manager.write().await?;
        let index = bm.update_and_get_index().await?;
        let (oldest_record, latest_record) = if index.tree().is_empty() {
            (0, 0)
        } else {
            let latest_block_id = index.tree().last().unwrap();
            let latest_record = match index.get_block(*latest_block_id) {
                Some(block) => ts_to_us(&block.latest_record_time.as_ref().unwrap()),
                None => 0,
            };
            (*index.tree().first().unwrap(), latest_record)
        };

        let status = status_result.await?;

        Ok(EntryInfo {
            name,
            size: index.size(),
            record_count: index.record_count(),
            block_count: index.tree().len() as u64,
            oldest_record,
            latest_record,
            status,
        })
    }

    pub(crate) async fn status(&self) -> Result<ResourceStatus, ReductError> {
        Ok(*self.status.read().await?)
    }

    pub(crate) async fn mark_deleting(&self) -> Result<(), ReductError> {
        self.ensure_not_deleting().await?;
        *self.status.write().await? = ResourceStatus::Deleting;
        Ok(())
    }

    pub(crate) async fn ensure_not_deleting(&self) -> Result<(), ReductError> {
        if self.status().await? == ResourceStatus::Deleting {
            Err(conflict!(
                "Entry '{}' in bucket '{}' is being deleted",
                self.name,
                self.bucket_name
            ))
        } else {
            Ok(())
        }
    }

    pub(super) async fn remove_all_blocks(&self) -> Result<(), ReductError> {
        let mut block_manager = self.block_manager.write().await?;
        block_manager.update_and_get_index().await?;
        let block_ids: Vec<u64> = block_manager.index().tree().iter().copied().collect();
        for block_id in block_ids {
            block_manager.remove_block(block_id).await?;
        }
        Ok(())
    }

    pub async fn size(&self) -> Result<u64, ReductError> {
        let bm = self.block_manager.read().await?;
        Ok(bm.index().size())
    }

    /// Try to remove the oldest block.
    ///
    /// # Returns
    ///
    /// HTTTPError - The error if any.
    pub async fn try_remove_oldest_block(&self) -> Result<(), ReductError> {
        let bm = self.block_manager.read().await?;
        let index_tree = bm.index().tree();
        if index_tree.is_empty() {
            return Err(internal_server_error!("No block to remove")).into();
        }

        let oldest_block_id = *index_tree.first().unwrap();
        let block_manager = Arc::clone(&self.block_manager);
        drop(bm); // release read lock before acquiring write lock

        let mut bm = match block_manager.write().await {
            Ok(bm) => bm,
            Err(_) => {
                return Err(internal_server_error!(
                    "Cannot remove block {} because it is in use",
                    oldest_block_id
                ));
            }
        };

        bm.remove_block(oldest_block_id).await.unwrap_or_else(|e| {
            error!("Failed to remove oldest block {}: {}", oldest_block_id, e);
        });
        debug!(
            "Removing the oldest block {}.blk",
            bm.path().join(oldest_block_id.to_string()).display()
        );

        Ok(())
    }

    // Compacts the entry by saving the block manager cache on disk and update index from WALs
    pub async fn compact(&self) -> Result<(), ReductError> {
        if let Some(mut bm) = self.block_manager.try_write() {
            bm.save_cache_on_disk().await
        } else {
            // Avoid blocking writers; we'll try again on the next sync tick
            debug!(
                "Skipping compact for {}/{} because block manager is busy",
                self.bucket_name, self.name
            );
            Ok(())
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn settings(&self) -> Result<EntrySettings, ReductError> {
        Ok(self.settings.read().await?.clone())
    }

    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    pub async fn set_settings(&self, settings: EntrySettings) -> Result<(), ReductError> {
        *self.settings.write().await? = settings;
        Ok(())
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    async fn remove_expired_query(
        queries: QueryHandleMapRef,
        entry_path: String,
    ) -> Result<(), ReductError> {
        queries.write().await?.retain(|id, handle| {
            if handle.last_access.elapsed() >= handle.options.ttl {
                debug!("Query {}/{} expired", entry_path, id);
                return false;
            }
            true
        });

        Ok(())
    }

    fn task_group(&self) -> String {
        self.path.display().to_string()
    }

    async fn get_query_time_range(&self, query: &QueryEntry) -> Result<(u64, u64), ReductError> {
        let info = self.info().await?;
        let start = if let Some(start) = query.start {
            start
        } else {
            info.oldest_record
        };

        let end = if let Some(end) = query.stop {
            end
        } else {
            info.latest_record + 1
        };

        Ok((start, end))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use reduct_base::{conflict, Labels};
    use rstest::{fixture, rstest};
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile;

    mod deleting {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn mark_deleting_returns_conflict_when_already_deleting(#[future] entry: Arc<Entry>) {
            let entry = entry.await;
            entry.mark_deleting().await.unwrap();
            assert_eq!(
                entry.mark_deleting().await,
                Err(conflict!(
                    "Entry '{}' in bucket '{}' is being deleted",
                    entry.name(),
                    entry.bucket_name()
                ))
            );
        }
    }

    mod restore {
        use super::*;
        use crate::storage::proto::{record, us_to_ts, Record};

        #[rstest]
        #[tokio::test]
        async fn test_restore(entry_settings: EntrySettings, path: PathBuf) {
            let entry = entry(entry_settings.clone(), path.clone()).await;
            write_stub_record(&entry, 1).await;
            write_stub_record(&entry, 2000010).await;

            let mut bm = entry.block_manager.write().await.unwrap();
            let records = bm
                .load_block(1)
                .await
                .unwrap()
                .read()
                .await
                .unwrap()
                .record_index()
                .clone();
            assert_eq!(records.len(), 2);
            assert_eq!(
                *records.get(&1).unwrap(),
                Record {
                    timestamp: Some(us_to_ts(&1)),
                    begin: 0,
                    end: 10,
                    content_type: "text/plain".to_string(),
                    state: record::State::Finished as i32,
                    labels: vec![],
                }
            );

            assert_eq!(
                *records.get(&2000010).unwrap(),
                Record {
                    timestamp: Some(us_to_ts(&2000010)),
                    begin: 10,
                    end: 20,
                    content_type: "text/plain".to_string(),
                    state: record::State::Finished as i32,
                    labels: vec![],
                }
            );

            bm.save_cache_on_disk().await.unwrap();
            let entry = Entry::restore(
                path.join(entry.name()),
                entry_settings,
                Cfg::default().into(),
            )
            .await
            .unwrap()
            .unwrap();
            let info = entry.info().await.unwrap();
            assert_eq!(info.name, "entry");
            assert_eq!(info.record_count, 2);
            assert_eq!(info.size, 88);
        }
    }

    mod query {
        use super::*;

        use reduct_base::error::ErrorCode;
        use reduct_base::io::ReadRecord;
        use reduct_base::{no_content, not_found};

        #[rstest]
        #[tokio::test]
        async fn test_historical_query(#[future] entry: Arc<Entry>) {
            let entry = entry.await;
            write_stub_record(&entry, 1000000).await;
            write_stub_record(&entry, 2000000).await;
            write_stub_record(&entry, 3000000).await;
            let ttl_s = 1;

            let params = QueryEntry {
                start: Some(0),
                stop: Some(4000000),
                ttl: Some(ttl_s),
                ..Default::default()
            };

            let id = entry.query(params).await.unwrap();
            assert!(id >= 1);

            {
                let (rx, _) = entry.get_query_receiver(id).await.unwrap();
                let rx = rx.upgrade_and_unwrap();
                let mut rx = rx.write().await.unwrap();

                {
                    let reader = rx.recv().await.unwrap().unwrap();
                    assert_eq!(reader.meta().timestamp(), 1000000);
                }
                {
                    let reader = rx.recv().await.unwrap().unwrap();
                    assert_eq!(reader.meta().timestamp(), 2000000);
                }
                {
                    let reader = rx.recv().await.unwrap().unwrap();
                    assert_eq!(reader.meta().timestamp(), 3000000);
                }

                assert_eq!(
                    rx.recv().await.unwrap().err(),
                    Some(no_content!("No content"))
                );
            }

            tokio::time::sleep(Duration::from_secs(ttl_s * 2)).await; // let query task finish

            assert_eq!(
                entry.get_query_receiver(id).await.err(),
                Some(not_found!(
                    "Query {} not found and it might have expired. Check TTL in your query request.",
                    id
                ))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_continuous_query(#[future] entry: Arc<Entry>) {
            let entry = entry.await;
            write_stub_record(&entry, 1000000).await;

            let params = QueryEntry {
                start: Some(0),
                stop: Some(4000000),
                continuous: Some(true),
                ttl: Some(1),
                ..Default::default()
            };
            let id = entry.query(params).await.unwrap();

            {
                let (rx, _) = entry.get_query_receiver(id).await.unwrap();
                let rx = rx.upgrade_and_unwrap();
                let mut rx = rx.write().await.unwrap();
                let reader = rx.recv().await.unwrap().unwrap();
                assert_eq!(reader.meta().timestamp(), 1000000);
                assert_eq!(
                    rx.recv().await.unwrap().err(),
                    Some(no_content!("No content"))
                );
            }

            write_stub_record(&entry, 2000000).await;
            {
                let (rx, _) = entry.get_query_receiver(id).await.unwrap();
                let rc = rx.upgrade_and_unwrap();
                let mut rx = rc.write().await.unwrap();
                let reader = loop {
                    let reader = rx.recv().await.unwrap();
                    match reader {
                        Ok(reader) => break reader,
                        Err(ReductError {
                            status: ErrorCode::NoContent,
                            ..
                        }) => continue,
                        Err(e) => panic!("Unexpected error: {:?}", e),
                    }
                };
                assert_eq!(reader.meta().timestamp(), 2000000);
            }

            tokio::time::sleep(Duration::from_millis(1700)).await;
            assert_eq!(
                entry.get_query_receiver(id).await.err(),
                Some(not_found!(
                    "Query {} not found and it might have expired. Check TTL in your query request.",
                    id
                ))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn keep_finished_query_until_ttl(#[future] entry: Arc<Entry>) {
            let entry = entry.await;
            write_stub_record(&entry, 1000000).await;

            let id = entry
                .query(QueryEntry {
                    limit: Some(1),
                    ttl: Some(1),
                    ..Default::default()
                })
                .await
                .unwrap();

            let (rx, _) = entry.get_query_receiver(id).await.unwrap();
            let rx = rx.upgrade_and_unwrap();
            {
                let mut rx = rx.write().await.unwrap();
                while rx.try_recv().is_ok() {}
            }

            for _ in 0..10 {
                let finished = entry
                    .queries
                    .read()
                    .await
                    .unwrap()
                    .get(&id)
                    .map(|handle| handle.query_task_handle.is_finished())
                    .unwrap_or(false);
                if finished {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            Entry::remove_expired_query(
                Arc::clone(&entry.queries),
                format!("{}/{}", entry.bucket_name(), entry.name()),
            )
            .await
            .unwrap();
            assert!(entry.queries.read().await.unwrap().contains_key(&id));

            tokio::time::sleep(Duration::from_secs(2)).await;
            Entry::remove_expired_query(
                Arc::clone(&entry.queries),
                format!("{}/{}", entry.bucket_name(), entry.name()),
            )
            .await
            .unwrap();
            assert!(!entry.queries.read().await.unwrap().contains_key(&id));
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_info(path: PathBuf) {
        let entry = entry(
            EntrySettings {
                max_block_size: 10000,
                max_block_records: 10000,
            },
            path,
        )
        .await;

        write_stub_record(&entry, 1000000).await;
        write_stub_record(&entry, 2000000).await;
        write_stub_record(&entry, 3000000).await;

        let info = entry.info().await.unwrap();
        assert_eq!(info.name, "entry");
        assert_eq!(info.size, 88);
        assert_eq!(info.record_count, 3);
        assert_eq!(info.block_count, 1);
        assert_eq!(info.oldest_record, 1000000);
        assert_eq!(info.latest_record, 3000000);
    }

    mod try_remove_oldest_block {
        use super::*;

        use crate::storage::engine::{CHANNEL_BUFFER_SIZE, MAX_IO_BUFFER_SIZE};

        #[rstest]
        #[tokio::test]
        async fn test_empty_entry(#[future] entry: Arc<Entry>) {
            let entry = entry.await;
            assert_eq!(
                entry.try_remove_oldest_block().await,
                Err(internal_server_error!("No block to remove"))
            );
        }

        #[rstest]
        #[ignore] // experimental:  without reader protection.
        #[tokio::test]
        async fn test_entry_which_has_reader(#[future] entry: Arc<Entry>) {
            let entry = entry.await;
            write_record(
                &entry,
                1000000,
                vec![0; MAX_IO_BUFFER_SIZE * CHANNEL_BUFFER_SIZE + 1],
            )
            .await;
            let _rx = entry.begin_read(1000000).await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;

            assert!(entry
                .try_remove_oldest_block()
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("because it is in use"));
            let info = entry.info().await.unwrap();
            assert_eq!(info.block_count, 1);
            assert_eq!(info.size, 8388630);
        }

        #[rstest]
        #[ignore] // experimental:  without writer protection.
        #[tokio::test]
        async fn test_entry_which_has_writer(#[future] entry: Arc<Entry>) {
            let entry = entry.await;
            let mut sender = entry
                .clone()
                .begin_write(
                    1000000,
                    (MAX_IO_BUFFER_SIZE + 1) as u64,
                    "text/plain".to_string(),
                    Labels::new(),
                )
                .await
                .unwrap();
            sender
                .send(Ok(Some(Bytes::from_static(b"456789"))))
                .await
                .unwrap();

            tokio::time::sleep(Duration::from_millis(100)).await;
            assert_eq!(
                entry.try_remove_oldest_block().await,
                Err(internal_server_error!(
                    "Cannot remove block 1000000 because it is still in use"
                ))
            );
            let info = entry.info().await.unwrap();
            assert_eq!(info.block_count, 1);
            assert_eq!(info.size, 524309);
        }

        #[rstest]
        #[tokio::test]
        async fn test_size_counting(path: PathBuf) {
            let entry = Arc::new(
                Entry::try_build(
                    "entry",
                    path.clone(),
                    EntrySettings {
                        max_block_size: 100000,
                        max_block_records: 2,
                    },
                    Cfg::default().into(),
                )
                .await
                .unwrap(),
            );

            write_stub_record(&entry, 1000000).await;
            write_stub_record(&entry, 2000000).await;
            write_stub_record(&entry, 3000000).await;
            write_stub_record(&entry, 4000000).await;

            assert_eq!(entry.info().await.unwrap().block_count, 2);
            assert_eq!(entry.info().await.unwrap().record_count, 4);
            assert_eq!(entry.info().await.unwrap().size, 116);

            entry.try_remove_oldest_block().await.unwrap();
            assert_eq!(entry.info().await.unwrap().block_count, 1);
            assert_eq!(entry.info().await.unwrap().record_count, 2);
            assert_eq!(entry.info().await.unwrap().size, 58);

            entry.try_remove_oldest_block().await.unwrap();
            assert_eq!(entry.info().await.unwrap().block_count, 0);
            assert_eq!(entry.info().await.unwrap().record_count, 0);
            assert_eq!(entry.info().await.unwrap().size, 0);
        }
    }

    #[fixture]
    pub(super) fn entry_settings() -> EntrySettings {
        EntrySettings {
            max_block_size: 10000,
            max_block_records: 10000,
        }
    }

    #[fixture]
    pub(super) async fn entry(entry_settings: EntrySettings, path: PathBuf) -> Arc<Entry> {
        Arc::new(
            Entry::try_build("entry", path.clone(), entry_settings, Cfg::default().into())
                .await
                .unwrap(),
        )
    }

    #[fixture]
    pub(super) fn path() -> PathBuf {
        tempfile::tempdir().unwrap().keep().join("bucket")
    }

    pub async fn write_record(entry: &Arc<Entry>, time: u64, data: Vec<u8>) {
        let mut sender = entry
            .clone()
            .begin_write(
                time,
                data.len() as u64,
                "text/plain".to_string(),
                Labels::new(),
            )
            .await
            .unwrap();
        sender.send(Ok(Some(Bytes::from(data)))).await.unwrap();
        sender.send(Ok(None)).await.expect("Failed to send None");
        drop(sender);
        tokio::time::sleep(Duration::from_millis(25)).await; // let the record be written
    }

    pub async fn write_record_with_labels(
        entry: &Arc<Entry>,
        time: u64,
        data: Vec<u8>,
        labels: Labels,
    ) {
        let mut sender = entry
            .clone()
            .begin_write(time, data.len() as u64, "text/plain".to_string(), labels)
            .await
            .unwrap();
        sender.send(Ok(Some(Bytes::from(data)))).await.unwrap();
        sender.send(Ok(None)).await.expect("Failed to send None");
    }

    pub(super) async fn write_stub_record(entry: &Arc<Entry>, time: u64) {
        write_record(entry, time, b"0123456789".to_vec()).await;
    }
}
