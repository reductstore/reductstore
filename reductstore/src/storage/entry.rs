// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod entry_loader;
pub(crate) mod io;
mod read_record;
mod remove_records;
pub(crate) mod update_labels;
mod write_record;

use crate::cfg::io::IoConfig;
use crate::cfg::Cfg;
use crate::core::sync::{AsyncRwLock, RwLock};
use crate::core::thread_pool::{spawn, TaskHandle};
use crate::core::weak::Weak;
use crate::storage::block_manager::block_index::BlockIndex;
use crate::storage::block_manager::{BlockManager, BLOCK_INDEX_FILE};
use crate::storage::entry::entry_loader::EntryLoader;
use crate::storage::proto::ts_to_us;
use crate::storage::query::base::QueryOptions;
use crate::storage::query::{build_query, spawn_query_task, QueryRx};
use log::debug;
use reduct_base::error::ReductError;
use reduct_base::msg::entry_api::{EntryInfo, QueryEntry};
use reduct_base::msg::status::ResourceStatus;
use reduct_base::{conflict, internal_server_error, not_found};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

pub(crate) use io::record_reader::RecordReader;
pub(crate) use io::record_writer::{RecordDrainer, RecordWriter};

struct QueryHandle {
    rx: Arc<AsyncRwLock<QueryRx>>,
    options: QueryOptions,
    last_access: Instant,
    #[allow(dead_code)]
    query_task_handle: TaskHandle<()>,
    io_settings: IoConfig,
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
    status: RwLock<ResourceStatus>,
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
    pub fn try_new(
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
            settings: RwLock::new(settings),
            block_manager: Arc::new(RwLock::new(BlockManager::new(
                path.clone(),
                BlockIndex::new(path.join(BLOCK_INDEX_FILE)),
                cfg.clone(),
            ))),
            queries: Arc::new(RwLock::new(HashMap::new())),
            status: RwLock::new(ResourceStatus::Ready),
            path,
            cfg,
        })
    }

    pub(crate) fn restore(
        path: PathBuf,
        options: EntrySettings,
        cfg: Arc<Cfg>,
    ) -> TaskHandle<Result<Option<Entry>, ReductError>> {
        spawn("restore entry", move || {
            let entry = EntryLoader::restore_entry(path, options, cfg)?;
            Ok(entry)
        })
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
    pub fn query(&self, query_parameters: QueryEntry) -> Result<u64, ReductError> {
        static QUERY_ID: AtomicU64 = AtomicU64::new(1); // start with 1 because 0 may confuse with false

        let (start, stop) = self.get_query_time_range(&query_parameters)?;
        let id = QUERY_ID.fetch_add(1, Ordering::SeqCst);
        let block_manager = Arc::clone(&self.block_manager);

        let options: QueryOptions = query_parameters.into();
        let query = build_query(start, stop, options.clone(), self.cfg.io_conf.clone())?;

        let io_settings = query.as_ref().io_settings().clone();
        let (rx, task_handle) =
            spawn_query_task(id, self.task_group(), query, options.clone(), block_manager);

        self.queries.write()?.insert(
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
    pub fn get_query_receiver(
        &self,
        query_id: u64,
    ) -> Result<(Weak<AsyncRwLock<QueryRx>>, IoConfig), ReductError> {
        let entry_path = format!("{}/{}", self.bucket_name, self.name);
        let queries = Arc::clone(&self.queries);
        spawn("remove expired queries", move || {
            Self::remove_expired_query(queries, entry_path);
        })
        .wait();

        let mut queries = self.queries.write()?;
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
    pub fn info(&self) -> Result<EntryInfo, ReductError> {
        let name = self.name.clone();
        let status_result = self.status();

        let mut bm = self.block_manager.write()?;
        let index = bm.update_and_get_index()?;
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

        let status = status_result?;

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

    pub(crate) fn status(&self) -> Result<ResourceStatus, ReductError> {
        Ok(*self.status.read()?)
    }

    pub(crate) fn mark_deleting(&self) -> Result<(), ReductError> {
        self.ensure_not_deleting()?;
        *self.status.write()? = ResourceStatus::Deleting;
        Ok(())
    }

    pub(crate) fn ensure_not_deleting(&self) -> Result<(), ReductError> {
        if self.status()? == ResourceStatus::Deleting {
            Err(conflict!(
                "Entry '{}' in bucket '{}' is being deleted",
                self.name,
                self.bucket_name
            ))
        } else {
            Ok(())
        }
    }

    pub fn size(&self) -> Result<u64, ReductError> {
        let bm = self.block_manager.read()?;
        Ok(bm.index().size())
    }

    /// Try to remove the oldest block.
    ///
    /// # Returns
    ///
    /// HTTTPError - The error if any.
    pub fn try_remove_oldest_block(&self) -> TaskHandle<Result<(), ReductError>> {
        let bm = match self.block_manager.read() {
            Ok(bm) => bm,
            Err(e) => return Err(e).into(),
        };

        let index_tree = bm.index().tree();
        if index_tree.is_empty() {
            return Err(internal_server_error!("No block to remove")).into();
        }

        let oldest_block_id = *index_tree.first().unwrap();
        let block_manager = Arc::clone(&self.block_manager);
        drop(bm); // release read lock before acquiring write lock

        spawn("remove oldest block", move || {
            let mut bm = block_manager.write()?;
            bm.remove_block(oldest_block_id)?;
            debug!(
                "Removing the oldest block {}.blk",
                bm.path().join(oldest_block_id.to_string()).display()
            );
            Ok(())
        })
    }

    // Compacts the entry by saving the block manager cache on disk and update index from WALs
    pub fn compact(&self) -> Result<(), ReductError> {
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
            true
        });
    }

    fn task_group(&self) -> String {
        self.path.display().to_string()
    }

    fn get_query_time_range(&self, query: &QueryEntry) -> Result<(u64, u64), ReductError> {
        let info = self.info()?;
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
    use std::thread::sleep;
    use std::time::Duration;
    use tempfile;

    mod deleting {
        use super::*;

        #[rstest]
        fn mark_deleting_returns_conflict_when_already_deleting(entry: Entry) {
            entry.mark_deleting().unwrap();
            assert_eq!(
                entry.mark_deleting(),
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
        fn test_restore(entry_settings: EntrySettings, path: PathBuf) {
            let mut entry = entry(entry_settings.clone(), path.clone());
            write_stub_record(&mut entry, 1);
            write_stub_record(&mut entry, 2000010);

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
            let entry =
                Entry::restore(path.join(entry.name), entry_settings, Cfg::default().into())
                    .wait()
                    .unwrap()
                    .unwrap();
            let info = entry.info().unwrap();
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
        use std::thread::sleep;

        #[rstest]
        fn test_historical_query(mut entry: Entry) {
            write_stub_record(&mut entry, 1000000);
            write_stub_record(&mut entry, 2000000);
            write_stub_record(&mut entry, 3000000);
            let ttl_s = 1;

            let params = QueryEntry {
                start: Some(0),
                stop: Some(4000000),
                ttl: Some(ttl_s),
                ..Default::default()
            };

            let id = entry.query(params).unwrap();
            assert!(id >= 1);

            {
                let (rx, _) = entry.get_query_receiver(id).unwrap();
                let rx = rx.upgrade_and_unwrap();
                let mut rx = rx.blocking_write().unwrap();

                {
                    let reader = rx.blocking_recv().unwrap().unwrap();
                    assert_eq!(reader.meta().timestamp(), 1000000);
                }
                {
                    let reader = rx.blocking_recv().unwrap().unwrap();
                    assert_eq!(reader.meta().timestamp(), 2000000);
                }
                {
                    let reader = rx.blocking_recv().unwrap().unwrap();
                    assert_eq!(reader.meta().timestamp(), 3000000);
                }

                assert_eq!(
                    rx.blocking_recv().unwrap().err(),
                    Some(no_content!("No content"))
                );
            }

            sleep(Duration::from_secs(ttl_s * 2)); // let query task finish

            assert_eq!(
                entry.get_query_receiver(id).err(),
                Some(not_found!("Query {} not found and it might have expired. Check TTL in your query request.", id))
            );
        }

        #[rstest]
        fn test_continuous_query(mut entry: Entry) {
            write_stub_record(&mut entry, 1000000);

            let params = QueryEntry {
                start: Some(0),
                stop: Some(4000000),
                continuous: Some(true),
                ttl: Some(1),
                ..Default::default()
            };
            let id = entry.query(params).unwrap();

            {
                let (rx, _) = entry.get_query_receiver(id).unwrap();
                let rx = rx.upgrade_and_unwrap();
                let mut rx = rx.blocking_write().unwrap();
                let reader = rx.blocking_recv().unwrap().unwrap();
                assert_eq!(reader.meta().timestamp(), 1000000);
                assert_eq!(
                    rx.blocking_recv().unwrap().err(),
                    Some(no_content!("No content"))
                );
            }

            write_stub_record(&mut entry, 2000000);
            {
                let (rx, _) = entry.get_query_receiver(id).unwrap();
                let rc = rx.upgrade_and_unwrap();
                let mut rx = rc.blocking_write().unwrap();
                let reader = loop {
                    let reader = rx.blocking_recv().unwrap();
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

            sleep(Duration::from_millis(1700));
            assert_eq!(
                entry.get_query_receiver(id).err(),
                Some(not_found!("Query {} not found and it might have expired. Check TTL in your query request.", id))
            );
        }

        #[rstest]
        fn keep_finished_query_until_ttl(mut entry: Entry) {
            write_stub_record(&mut entry, 1000000);

            let id = entry
                .query(QueryEntry {
                    limit: Some(1),
                    ttl: Some(1),
                    ..Default::default()
                })
                .wait()
                .unwrap();

            let (rx, _) = entry.get_query_receiver(id).unwrap();
            let rx = rx.upgrade_and_unwrap();
            {
                let mut rx = rx.blocking_write().unwrap();
                while rx.try_recv().is_ok() {}
            }

            for _ in 0..10 {
                let finished = entry
                    .queries
                    .read()
                    .unwrap()
                    .get(&id)
                    .map(|handle| handle.query_task_handle.is_finished())
                    .unwrap_or(false);
                if finished {
                    break;
                }
                sleep(Duration::from_millis(10));
            }

            Entry::remove_expired_query(
                Arc::clone(&entry.queries),
                format!("{}/{}", entry.bucket_name(), entry.name()),
            );
            assert!(entry.queries.read().unwrap().contains_key(&id));

            sleep(Duration::from_secs(2));
            Entry::remove_expired_query(
                Arc::clone(&entry.queries),
                format!("{}/{}", entry.bucket_name(), entry.name()),
            );
            assert!(!entry.queries.read().unwrap().contains_key(&id));
        }
    }

    #[rstest]
    fn test_info(path: PathBuf) {
        let mut entry = entry(
            EntrySettings {
                max_block_size: 10000,
                max_block_records: 10000,
            },
            path,
        );

        write_stub_record(&mut entry, 1000000);
        write_stub_record(&mut entry, 2000000);
        write_stub_record(&mut entry, 3000000);

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
        use std::thread::sleep;

        use crate::storage::engine::{CHANNEL_BUFFER_SIZE, MAX_IO_BUFFER_SIZE};

        #[rstest]
        fn test_empty_entry(entry: Entry) {
            assert_eq!(
                entry.try_remove_oldest_block().wait(),
                Err(internal_server_error!("No block to remove"))
            );
        }

        #[rstest]
        #[ignore] // experimental:  without reader protection.
        fn test_entry_which_has_reader(mut entry: Entry) {
            write_record(
                &mut entry,
                1000000,
                vec![0; MAX_IO_BUFFER_SIZE * CHANNEL_BUFFER_SIZE + 1],
            );
            let _rx = entry.begin_read(1000000).wait().unwrap();
            sleep(Duration::from_millis(100));

            assert!(entry
                .try_remove_oldest_block()
                .wait()
                .err()
                .unwrap()
                .to_string()
                .contains("because it is in use"));
            let info = entry.info().unwrap();
            assert_eq!(info.block_count, 1);
            assert_eq!(info.size, 8388630);
        }

        #[rstest]
        #[ignore] // experimental:  without writer protection.
        fn test_entry_which_has_writer(entry: Entry) {
            let mut sender = entry
                .begin_write(
                    1000000,
                    (MAX_IO_BUFFER_SIZE + 1) as u64,
                    "text/plain".to_string(),
                    Labels::new(),
                )
                .wait()
                .unwrap();
            sender
                .blocking_send(Ok(Some(Bytes::from_static(b"456789"))))
                .unwrap();

            sleep(Duration::from_millis(100));
            assert_eq!(
                entry.try_remove_oldest_block().wait(),
                Err(internal_server_error!(
                    "Cannot remove block 1000000 because it is still in use"
                ))
            );
            let info = entry.info().unwrap();
            assert_eq!(info.block_count, 1);
            assert_eq!(info.size, 524309);
        }

        #[rstest]
        fn test_size_counting(path: PathBuf) {
            let mut entry = Entry::try_new(
                "entry",
                path.clone(),
                EntrySettings {
                    max_block_size: 100000,
                    max_block_records: 2,
                },
                Cfg::default().into(),
            )
            .unwrap();

            write_stub_record(&mut entry, 1000000);
            write_stub_record(&mut entry, 2000000);
            write_stub_record(&mut entry, 3000000);
            write_stub_record(&mut entry, 4000000);

            assert_eq!(entry.info().unwrap().block_count, 2);
            assert_eq!(entry.info().unwrap().record_count, 4);
            assert_eq!(entry.info().unwrap().size, 116);

            entry.try_remove_oldest_block().wait().unwrap();
            assert_eq!(entry.info().unwrap().block_count, 1);
            assert_eq!(entry.info().unwrap().record_count, 2);
            assert_eq!(entry.info().unwrap().size, 58);

            entry.try_remove_oldest_block().wait().unwrap();
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
        Entry::try_new("entry", path.clone(), entry_settings, Cfg::default().into()).unwrap()
    }

    #[fixture]
    pub(super) fn path() -> PathBuf {
        tempfile::tempdir().unwrap().keep().join("bucket")
    }

    pub fn write_record(entry: &mut Entry, time: u64, data: Vec<u8>) {
        let mut sender = entry
            .begin_write(
                time,
                data.len() as u64,
                "text/plain".to_string(),
                Labels::new(),
            )
            .wait()
            .unwrap();
        sender.blocking_send(Ok(Some(Bytes::from(data)))).unwrap();
        sender.blocking_send(Ok(None)).expect("Failed to send None");
        drop(sender);
        sleep(Duration::from_millis(25)); // let the record be written
    }

    pub fn write_record_with_labels(entry: &mut Entry, time: u64, data: Vec<u8>, labels: Labels) {
        let mut sender = entry
            .begin_write(time, data.len() as u64, "text/plain".to_string(), labels)
            .wait()
            .unwrap();
        sender.blocking_send(Ok(Some(Bytes::from(data)))).unwrap();
        sender.blocking_send(Ok(None)).expect("Failed to send None");
    }

    pub(super) fn write_stub_record(entry: &mut Entry, time: u64) {
        write_record(entry, time, b"0123456789".to_vec());
    }
}
