// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod entry_loader;
pub(crate) mod io;
mod read_record;
mod remove_records;
pub(crate) mod update_labels;
mod write_record;

use crate::storage::block_manager::block_index::BlockIndex;
use crate::storage::block_manager::{BlockManager, BLOCK_INDEX_FILE};
use crate::storage::entry::entry_loader::EntryLoader;
use crate::storage::proto::ts_to_us;
use crate::storage::query::base::QueryOptions;
use crate::storage::query::{build_query, spawn_query_task, QueryRx};
use log::debug;
use reduct_base::error::ReductError;
use reduct_base::msg::entry_api::EntryInfo;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tokio::sync::RwLock as AsyncRwLock;

pub(crate) use io::record_writer::{RecordDrainer, RecordWriter, WriteRecordContent};

use crate::core::thread_pool::{shared, try_unique, unique, TaskHandle};
use crate::core::weak::Weak;
pub(crate) use io::record_reader::RecordReader;
use reduct_base::internal_server_error;

struct QueryHandle {
    rx: Arc<AsyncRwLock<QueryRx>>,
    options: QueryOptions,
    last_access: Instant,
    query_task_handle: TaskHandle<()>,
}

type QueryHandleMap = HashMap<u64, QueryHandle>;
type QueryHandleMapRef = Arc<RwLock<QueryHandleMap>>;

/// Entry is a time series in a bucket.
pub(crate) struct Entry {
    name: String,
    bucket_name: String,
    settings: RwLock<EntrySettings>,
    block_manager: Arc<RwLock<BlockManager>>,
    queries: QueryHandleMapRef,
    path: PathBuf,
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
    pub fn try_new(
        name: &str,
        path: PathBuf,
        settings: EntrySettings,
    ) -> Result<Self, ReductError> {
        fs::create_dir_all(path.join(name))?;
        let path = path.join(name);
        Ok(Self {
            name: name.to_string(),
            bucket_name: path.file_name().unwrap().to_str().unwrap().to_string(),
            settings: RwLock::new(settings),
            block_manager: Arc::new(RwLock::new(BlockManager::new(
                path.clone(),
                BlockIndex::new(path.join(BLOCK_INDEX_FILE)),
            ))),
            queries: Arc::new(RwLock::new(HashMap::new())),
            path,
        })
    }

    pub(crate) fn restore(
        path: PathBuf,
        options: EntrySettings,
    ) -> TaskHandle<Result<Entry, ReductError>> {
        let bucket_name = path.file_name().unwrap().to_str().unwrap().to_string();
        let name = path.file_name().unwrap().to_str().unwrap().to_string();
        unique(
            &[
                path.parent().unwrap().to_str().unwrap(),
                &bucket_name,
                &name,
            ]
            .join("/"),
            "restore entry",
            move || {
                let entry = EntryLoader::restore_entry(path, options)?;
                Ok(entry)
            },
        )
    }

    /// Query records for a time range.
    ///
    /// # Arguments
    ///
    /// * `start` - The start time of the query.
    /// * `end` - The end time of the query. Ignored if `continuous` is true.
    /// * `options` - The query options.
    ///
    /// # Returns
    ///
    /// * `u64` - The query ID.
    /// * `HTTPError` - The error if any.
    pub fn query(
        &self,
        start: u64,
        end: u64,
        options: QueryOptions,
    ) -> TaskHandle<Result<u64, ReductError>> {
        static QUERY_ID: AtomicU64 = AtomicU64::new(1); // start with 1 because 0 may confuse with false

        let id = QUERY_ID.fetch_add(1, Ordering::SeqCst);
        let block_manager = Arc::clone(&self.block_manager);

        let query = build_query(start, end, options.clone());
        if let Err(e) = query {
            return e.into();
        }

        let (rx, task_handle) = spawn_query_task(
            self.task_group(),
            query.unwrap(),
            options.clone(),
            block_manager,
        );

        self.queries.write().unwrap().insert(
            id,
            QueryHandle {
                rx: Arc::new(AsyncRwLock::new(rx)),
                options,
                last_access: Instant::now(),
                query_task_handle: task_handle,
            },
        );

        Ok(id).into()
    }

    /// Returns the next record for a query.
    ///
    /// # Arguments
    ///
    /// * `query_id` - The query ID.
    ///
    /// # Returns
    ///
    /// * `(RecordReader, bool)` - The record reader to read the record content in chunks and a boolean indicating if the query is done.
    /// * `HTTPError` - The error if any.
    pub fn get_query_receiver(
        &self,
        query_id: u64,
    ) -> Result<Weak<AsyncRwLock<QueryRx>>, ReductError> {
        let entry_path = format!("{}/{}", self.bucket_name, self.name);
        let queries = Arc::clone(&self.queries);
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        shared(&self.task_group(), "remove expired query", move || {
            Self::remove_expired_query(queries, entry_path);
            tx.send(()).unwrap();
        });

        rx.recv().unwrap();

        let mut queries = self.queries.write()?;

        let query = queries.get_mut(&query_id)
            .ok_or_else(||
                ReductError::not_found(
                    &format!("Query {} not found and it might have expired. Check TTL in your query request. Default value {} sec.", query_id, QueryOptions::default().ttl.as_secs())))?;

        query.last_access = Instant::now();
        Ok(Weak::new(Arc::clone(&query.rx)))
    }

    /// Returns stats about the entry.
    pub fn info(&self) -> Result<EntryInfo, ReductError> {
        let bm = self.block_manager.read()?;
        let index_tree = bm.index().tree();
        let (oldest_record, latest_record) = if index_tree.is_empty() {
            (0, 0)
        } else {
            let latest_block_id = index_tree.last().unwrap();
            let latest_record = match bm.index().get_block(*latest_block_id) {
                Some(block) => ts_to_us(&block.latest_record_time.as_ref().unwrap()),
                None => 0,
            };
            (*index_tree.first().unwrap(), latest_record)
        };

        Ok(EntryInfo {
            name: self.name.clone(),
            size: bm.index().size(),
            record_count: bm.index().record_count(),
            block_count: index_tree.len() as u64,
            oldest_record,
            latest_record,
        })
    }

    /// Try to remove the oldest block.
    ///
    /// # Returns
    ///
    /// HTTTPError - The error if any.
    pub fn try_remove_oldest_block(&self) -> TaskHandle<Result<(), ReductError>> {
        let bm = self.block_manager.read().unwrap();

        let index_tree = bm.index().tree();
        if index_tree.is_empty() {
            return Err(internal_server_error!("No block to remove")).into();
        }

        let oldest_block_id = *index_tree.first().unwrap();
        let block_manager = Arc::clone(&self.block_manager);

        match try_unique(
            &format!("{}/{}", self.task_group(), oldest_block_id),
            "try remove oldest block",
            move || {
                let mut bm = block_manager.write().unwrap();
                bm.remove_block(oldest_block_id)?;
                Ok(())
            },
        ) {
            Some(handle) => handle,
            None => Err(internal_server_error!(
                "Cannot remove block {} because it is still in use",
                oldest_block_id
            ))
            .into(),
        }
    }

    pub fn sync_fs(&self) -> Result<(), ReductError> {
        let mut bm = self.block_manager.write()?;
        bm.save_cache_on_disk()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn settings(&self) -> EntrySettings {
        self.settings.read().unwrap().clone()
    }

    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    pub fn set_settings(&self, settings: EntrySettings) {
        *self.settings.write().unwrap() = settings;
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    fn remove_expired_query(queries: QueryHandleMapRef, entry_path: String) {
        queries.write().unwrap().retain(|id, handle| {
            if handle.last_access.elapsed() >= handle.options.ttl {
                debug!("Query {}/{} expired", entry_path, id);
                return false;
            }

            if handle.rx.blocking_read().is_empty() && handle.query_task_handle.is_finished() {
                debug!("Query {}/{} finished", entry_path, id);
                return false;
            }

            true
        });
    }

    fn task_group(&self) -> String {
        // use folder hierarchy as task group to protect resources
        let bucket_path = self.path.parent().unwrap();
        let storage_path = bucket_path.parent().unwrap();

        [
            storage_path.file_name().unwrap().to_str().unwrap(),
            bucket_path.file_name().unwrap().to_str().unwrap(),
            self.path.file_name().unwrap().to_str().unwrap(),
        ]
        .join("/")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use reduct_base::Labels;
    use rstest::{fixture, rstest};
    use std::time::Duration;
    use tempfile;
    use tokio::time::sleep;

    mod restore {
        use super::*;
        use crate::storage::proto::{record, us_to_ts, Record};

        #[rstest]
        #[tokio::test]
        async fn test_restore(entry_settings: EntrySettings, path: PathBuf) {
            let mut entry = entry(entry_settings.clone(), path.clone());
            write_stub_record(&mut entry, 1).await.unwrap();
            write_stub_record(&mut entry, 2000010).await.unwrap();

            let mut bm = entry.block_manager.write().unwrap();
            let records = bm
                .load_block(1)
                .unwrap()
                .read()
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

            bm.save_cache_on_disk().unwrap();
            let entry = Entry::restore(path.join(entry.name), entry_settings)
                .await
                .unwrap();

            let info = entry.info().unwrap();
            assert_eq!(info.name, "entry");
            assert_eq!(info.record_count, 2);
            assert_eq!(info.size, 88);
        }
    }

    mod query {
        use super::*;
        use reduct_base::{no_content, not_found};

        #[rstest]
        #[tokio::test]
        async fn test_historical_query(mut entry: Entry) {
            write_stub_record(&mut entry, 1000000).await.unwrap();
            write_stub_record(&mut entry, 2000000).await.unwrap();
            write_stub_record(&mut entry, 3000000).await.unwrap();

            let id = entry
                .query(0, 4000000, QueryOptions::default())
                .await
                .unwrap();
            assert!(id >= 1);

            {
                let rx = entry.get_query_receiver(id).unwrap().upgrade_and_unwrap();
                let mut rx = rx.write().await;

                {
                    let reader = rx.recv().await.unwrap().unwrap();
                    assert_eq!(reader.timestamp(), 1000000);
                }
                {
                    let reader = rx.recv().await.unwrap().unwrap();
                    assert_eq!(reader.timestamp(), 2000000);
                }
                {
                    let reader = rx.recv().await.unwrap().unwrap();
                    assert_eq!(reader.timestamp(), 3000000);
                }

                assert_eq!(
                    rx.recv().await.unwrap().err(),
                    Some(no_content!("No content"))
                );
            }

            assert_eq!(
                entry.get_query_receiver(id).err(),
                Some(not_found!("Query {} not found and it might have expired. Check TTL in your query request. Default value 60 sec.", id))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_continuous_query(mut entry: Entry) {
            write_stub_record(&mut entry, 1000000).await.unwrap();

            let id = entry
                .query(
                    0,
                    4000000,
                    QueryOptions {
                        ttl: Duration::from_millis(500),
                        continuous: true,
                        ..QueryOptions::default()
                    },
                )
                .await
                .unwrap();

            {
                let rx = entry.get_query_receiver(id).unwrap().upgrade_and_unwrap();
                let mut rx = rx.write().await;
                let reader = rx.recv().await.unwrap().unwrap();
                assert_eq!(reader.timestamp(), 1000000);
                assert_eq!(
                    rx.recv().await.unwrap().err(),
                    Some(no_content!("No content"))
                );
            }

            write_stub_record(&mut entry, 2000000).await.unwrap();
            {
                let rx = entry.get_query_receiver(id).unwrap().upgrade_and_unwrap();
                let mut rx = rx.write().await;
                let reader = rx.recv().await.unwrap().unwrap();
                assert_eq!(reader.timestamp(), 2000000);
            }

            sleep(Duration::from_millis(700)).await;
            assert_eq!(
                entry.get_query_receiver(id).err(),
                Some(not_found!("Query {} not found and it might have expired. Check TTL in your query request. Default value 60 sec.", id))
            );
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_info(path: PathBuf) {
        let mut entry = entry(
            EntrySettings {
                max_block_size: 10000,
                max_block_records: 10000,
            },
            path,
        );

        write_stub_record(&mut entry, 1000000).await.unwrap();
        write_stub_record(&mut entry, 2000000).await.unwrap();
        write_stub_record(&mut entry, 3000000).await.unwrap();

        let info = entry.info().unwrap();
        assert_eq!(info.name, "entry");
        assert_eq!(info.size, 88);
        assert_eq!(info.record_count, 3);
        assert_eq!(info.block_count, 1);
        assert_eq!(info.oldest_record, 1000000);
        assert_eq!(info.latest_record, 3000000);
    }

    mod try_remove_oldest_block {
        use super::*;

        use crate::storage::storage::{CHANNEL_BUFFER_SIZE, MAX_IO_BUFFER_SIZE};

        #[rstest]
        fn test_empty_entry(entry: Entry) {
            assert_eq!(
                entry.try_remove_oldest_block().wait(),
                Err(internal_server_error!("No block to remove"))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_entry_which_has_reader(mut entry: Entry) {
            write_record(
                &mut entry,
                1000000,
                vec![0; MAX_IO_BUFFER_SIZE * CHANNEL_BUFFER_SIZE + 1],
            )
            .await
            .unwrap();
            let _rx = entry.begin_read(1000000).wait().unwrap();
            sleep(Duration::from_millis(100)).await;

            assert_eq!(
                entry.try_remove_oldest_block().wait(),
                Err(internal_server_error!(
                    "Cannot remove block 1000000 because it is still in use"
                ))
            );
            let info = entry.info().unwrap();
            assert_eq!(info.block_count, 1);
            assert_eq!(info.size, 8388630);
        }

        #[rstest]
        fn test_entry_which_has_writer(entry: Entry) {
            let sender = entry
                .begin_write(1000000, 10, "text/plain".to_string(), Labels::new())
                .wait()
                .unwrap();
            sender
                .tx()
                .blocking_send(Ok(Some(Bytes::from_static(b"456789"))))
                .unwrap();

            std::thread::sleep(Duration::from_millis(100));
            assert_eq!(
                entry.try_remove_oldest_block().wait(),
                Err(internal_server_error!(
                    "Cannot remove block 1000000 because it is still in use"
                ))
            );
            let info = entry.info().unwrap();
            assert_eq!(info.block_count, 1);
            assert_eq!(info.size, 28);
        }

        #[rstest]
        #[tokio::test]
        async fn test_size_counting(path: PathBuf) {
            let mut entry = Entry::try_new(
                "entry",
                path.clone(),
                EntrySettings {
                    max_block_size: 100000,
                    max_block_records: 2,
                },
            )
            .unwrap();

            write_stub_record(&mut entry, 1000000).await.unwrap();
            write_stub_record(&mut entry, 2000000).await.unwrap();
            write_stub_record(&mut entry, 3000000).await.unwrap();
            write_stub_record(&mut entry, 4000000).await.unwrap();

            assert_eq!(entry.info().unwrap().block_count, 2);
            assert_eq!(entry.info().unwrap().record_count, 4);
            assert_eq!(entry.info().unwrap().size, 138);

            entry.try_remove_oldest_block().await.unwrap();
            assert_eq!(entry.info().unwrap().block_count, 1);
            assert_eq!(entry.info().unwrap().record_count, 2);
            assert_eq!(entry.info().unwrap().size, 58);

            entry.try_remove_oldest_block().await.unwrap();
            assert_eq!(entry.info().unwrap().block_count, 0);
            assert_eq!(entry.info().unwrap().record_count, 0);
            assert_eq!(entry.info().unwrap().size, 0);
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
    pub(super) fn entry(entry_settings: EntrySettings, path: PathBuf) -> Entry {
        Entry::try_new("entry", path.clone(), entry_settings).unwrap()
    }

    #[fixture]
    pub(super) fn path() -> PathBuf {
        tempfile::tempdir().unwrap().into_path().join("bucket")
    }

    pub async fn write_record(
        entry: &mut Entry,
        time: u64,
        data: Vec<u8>,
    ) -> Result<(), ReductError> {
        let sender = entry
            .begin_write(time, data.len(), "text/plain".to_string(), Labels::new())
            .await?;
        let x = sender.tx().send(Ok(Some(Bytes::from(data)))).await;
        sender.tx().closed().await;
        drop(sender);
        match x {
            Ok(_) => Ok(()),
            Err(_) => Err(ReductError::internal_server_error("Error sending data")),
        }
    }

    pub async fn write_record_with_labels(
        entry: &mut Entry,
        time: u64,
        data: Vec<u8>,
        labels: Labels,
    ) -> Result<(), ReductError> {
        let sender = entry
            .begin_write(time, data.len(), "text/plain".to_string(), labels)
            .await?;
        let x = sender.tx().send(Ok(Some(Bytes::from(data)))).await;
        sender.tx().closed().await;
        match x {
            Ok(_) => Ok(()),
            Err(_) => Err(ReductError::internal_server_error("Error sending data")),
        }
    }

    pub(super) async fn write_stub_record(entry: &mut Entry, time: u64) -> Result<(), ReductError> {
        write_record(entry, time, b"0123456789".to_vec()).await
    }

    pub fn get_task_group(entry_path: &PathBuf, time: u64) -> String {
        let bucket_path = entry_path.parent().unwrap();
        let storage_path = bucket_path.parent().unwrap();
        format!(
            "{}/{}/{}/{}",
            storage_path.file_name().unwrap().to_str().unwrap(),
            bucket_path.file_name().unwrap().to_str().unwrap(),
            entry_path.file_name().unwrap().to_str().unwrap(),
            time
        )
    }
}
