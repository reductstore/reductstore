// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::block_manager::BlockRef;
use crate::storage::entry::{Entry, RecordType, RecordWriter};
use crate::storage::proto::{record, us_to_ts, Record};
use log::debug;
use reduct_base::error::ReductError;
use reduct_base::io::WriteRecord;
use reduct_base::Labels;
use reduct_macros::task;
use std::sync::Arc;

impl Entry {
    /// Starts a new record write.
    ///
    /// # Arguments
    ///
    /// * `time` - The timestamp of the record.
    /// * `content_size` - The size of the record content.
    /// * `content_type` - The content type of the record.
    /// * `labels` - The labels of the record.
    ///
    /// # Returns
    ///
    /// * `Sender<Result<Bytes, ReductError>>` - The sender to send the record content in chunks.
    /// * `HTTPError` - The error if any.
    #[task("begin write")]
    pub fn begin_write(
        self: Arc<Self>,
        time: u64,
        content_size: u64,
        content_type: String,
        labels: Labels,
    ) -> Result<Box<dyn WriteRecord + Sync + Send>, ReductError> {
        let settings = self.settings.read()?;
        let mut bm = self.block_manager.write()?;
        // When we write, the likely case is that we are writing the latest record
        // in the entry. In this case, we can just append to the latest block.
        let mut block_ref = if bm.index().tree().is_empty() {
            bm.start_new_block(time, settings.max_block_size)?
        } else {
            let block_id = *bm.index().tree().last().unwrap();
            bm.load_block(block_id)?
        };

        let record_type = {
            let is_belated = {
                let block = block_ref.write()?;
                block.record_count() > 0 && block.latest_record_time() >= time
            };
            if is_belated {
                debug!(
                    "Timestamp {} is belated for {}/{}. Looking for a block",
                    time, self.bucket_name, self.name
                );
                // The timestamp is belated. We need to find the proper block to write to.
                let index_tree = bm.index().tree();
                if *index_tree.first().unwrap() > time {
                    // The timestamp is the earliest. We need to create a new block.
                    debug!(
                        "Timestamp {} is the earliest for {}/{}. Creating a new block",
                        time, self.bucket_name, self.name
                    );
                    block_ref = bm.start_new_block(time, settings.max_block_size)?;
                    RecordType::BelatedFirst
                } else {
                    block_ref = bm.find_block(time)?;
                    let block_id = block_ref.read()?.block_id();
                    debug!(
                        "Timestamp {} is belated for {}/{}. Writing to block {}",
                        time, self.bucket_name, self.name, block_id
                    );
                    let record = block_ref.read()?.get_record(time).map(|r| r.clone());
                    // check if the record already exists
                    if let Some(mut record) = record {
                        // We overwrite the record if it is errored and the size is the same.
                        return if record.state != record::State::Errored as i32
                            || record.end - record.begin != content_size as u64
                        {
                            Err(ReductError::conflict(&format!(
                                "A record with timestamp {} already exists",
                                time
                            )))
                        } else {
                            {
                                let mut block = block_ref.write()?;
                                record.labels = labels
                                    .into_iter()
                                    .map(|(name, value)| record::Label { name, value })
                                    .collect();
                                record.state = record::State::Started as i32;
                                record.content_type = content_type;
                                block.insert_or_update_record(record);
                            }

                            drop(bm); // drop the lock to avoid deadlock
                            let writer = RecordWriter::try_new(
                                Arc::clone(&self.block_manager),
                                block_ref,
                                time,
                            )?;

                            return Ok(Box::new(writer));
                        };
                    }
                    RecordType::Belated
                }
            } else {
                // The timestamp is the latest. We can just append to the latest block.
                RecordType::Latest
            }
        };

        let mut block_ref = {
            let block = block_ref.read()?;
            // Check if the block has enough space for the record.
            let has_no_space = block.size() + content_size as u64 > settings.max_block_size;
            let has_too_many_records = block.record_count() + 1 > settings.max_block_records;

            drop(block);
            if record_type == RecordType::Latest && (has_no_space || has_too_many_records) {
                // We need to create a new block.
                debug!(
                    "Creating a new block for {}/{} (has_no_space={}, has_too_many_records={})",
                    self.bucket_name, self.name, has_no_space, has_too_many_records
                );
                bm.finish_block(block_ref.clone())?;
                bm.start_new_block(time, settings.max_block_size)?
            } else {
                // We can just append to the latest block.
                block_ref.clone()
            }
        };

        drop(bm);

        Self::prepare_block_for_writing(&mut block_ref, time, content_size, content_type, labels)?;

        let writer = RecordWriter::try_new(Arc::clone(&self.block_manager), block_ref, time)?;
        Ok(Box::new(writer))
    }

    fn prepare_block_for_writing(
        block: &mut BlockRef,
        time: u64,
        content_size: u64,
        content_type: String,
        labels: Labels,
    ) -> Result<(), ReductError> {
        let mut block = block.write()?;
        let record = Record {
            timestamp: Some(us_to_ts(&time)),
            begin: block.size(),
            end: block.size() + content_size,
            content_type,
            state: record::State::Started as i32,
            labels: labels
                .into_iter()
                .map(|(name, value)| record::Label { name, value })
                .collect(),
        };

        block.insert_or_update_record(record);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::entry::tests::{entry, path, write_stub_record};
    use crate::storage::entry::{Entry, EntrySettings};
    use crate::storage::proto::{record, us_to_ts, Record};
    use bytes::Bytes;
    use reduct_base::error::ReductError;
    use reduct_base::Labels;
    use rstest::rstest;
    use serial_test::serial;
    use std::path::PathBuf;
    use std::sync::Arc;

    #[rstest]
    #[serial]
    #[tokio::test]
    async fn test_begin_write_new_block_size(path: PathBuf) {
        let entry = entry(
            EntrySettings {
                max_block_size: 10,
                max_block_records: 10000,
            },
            path,
        );

        write_stub_record(&entry, 1).await;
        write_stub_record(&entry, 2000010).await;
        let mut bm = entry.block_manager.write().unwrap();

        assert_eq!(
            bm.load_block(1)
                .unwrap()
                .write()
                .unwrap()
                .get_record(1)
                .unwrap()
                .clone(),
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
            bm.load_block(2000010)
                .unwrap()
                .write()
                .unwrap()
                .get_record(2000010)
                .unwrap()
                .clone(),
            Record {
                timestamp: Some(us_to_ts(&2000010)),
                begin: 0,
                end: 10,
                content_type: "text/plain".to_string(),
                state: record::State::Finished as i32,
                labels: vec![],
            }
        );
    }

    #[rstest]
    #[serial]
    #[tokio::test]
    async fn test_begin_write_new_block_records(path: PathBuf) {
        let entry = entry(
            EntrySettings {
                max_block_size: 10000,
                max_block_records: 1,
            },
            path,
        );

        write_stub_record(&entry, 1).await;
        write_stub_record(&entry, 2).await;
        write_stub_record(&entry, 2000010).await;

        let mut bm = entry.block_manager.write().unwrap();
        let records = bm
            .load_block(1)
            .unwrap()
            .write()
            .unwrap()
            .record_index()
            .clone();
        assert_eq!(
            records.get(&1).unwrap().clone(),
            Record {
                timestamp: Some(us_to_ts(&1)),
                begin: 0,
                end: 10,
                content_type: "text/plain".to_string(),
                state: record::State::Finished as i32,
                labels: vec![],
            }
        );

        let records = bm
            .load_block(2000010)
            .unwrap()
            .write()
            .unwrap()
            .record_index()
            .clone();
        assert_eq!(
            records.get(&2000010).unwrap().clone(),
            Record {
                timestamp: Some(us_to_ts(&2000010)),
                begin: 0,
                end: 10,
                content_type: "text/plain".to_string(),
                state: record::State::Finished as i32,
                labels: vec![],
            }
        );
    }

    #[rstest]
    #[serial]
    #[tokio::test]
    async fn test_begin_write_belated_record(entry: Arc<Entry>) {
        write_stub_record(&entry, 1000000).await;
        write_stub_record(&entry, 3000000).await;
        write_stub_record(&entry, 2000000).await;

        let mut bm = entry.block_manager.write().unwrap();
        let records = bm
            .load_block(1000000)
            .unwrap()
            .read()
            .unwrap()
            .record_index()
            .clone();
        assert_eq!(records.len(), 3);
        assert_eq!(
            records.get(&1000000).unwrap().timestamp,
            Some(us_to_ts(&1000000))
        );
        assert_eq!(
            records.get(&2000000).unwrap().timestamp,
            Some(us_to_ts(&2000000))
        );
        assert_eq!(
            records.get(&3000000).unwrap().timestamp,
            Some(us_to_ts(&3000000))
        );
    }

    #[rstest]
    #[serial]
    #[tokio::test]
    async fn test_begin_write_belated_first(entry: Arc<Entry>) {
        write_stub_record(&entry, 3000000).await;
        write_stub_record(&entry, 1000000).await;

        let mut bm = entry.block_manager.write().unwrap();
        let records = bm
            .load_block(1000000)
            .unwrap()
            .read()
            .unwrap()
            .record_index()
            .clone();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records.get(&1000000).unwrap().timestamp,
            Some(us_to_ts(&1000000))
        );
    }

    #[rstest]
    #[serial]
    #[tokio::test]
    async fn test_begin_write_existing_record(entry: Arc<Entry>) {
        write_stub_record(&entry, 1000000).await;
        write_stub_record(&entry, 2000000).await;
        let err = entry
            .begin_write(1000000, 10, "text/plain".to_string(), Labels::new())
            .wait();
        assert_eq!(
            err.err(),
            Some(ReductError::conflict(
                "A record with timestamp 1000000 already exists"
            ))
        );
    }

    #[rstest]
    #[serial]
    #[tokio::test]
    async fn test_begin_write_existing_record_belated(entry: Arc<Entry>) {
        write_stub_record(&entry, 2000000).await;
        write_stub_record(&entry, 1000000).await;
        let err = entry
            .begin_write(1000000, 10, "text/plain".to_string(), Labels::new())
            .wait();
        assert_eq!(
            err.err(),
            Some(ReductError::conflict(
                "A record with timestamp 1000000 already exists"
            ))
        );
    }

    #[rstest]
    #[serial]
    #[tokio::test]
    async fn test_begin_override_errored(entry: Arc<Entry>) {
        let mut sender = entry
            .begin_write(1000000, 10, "text/plain".to_string(), Labels::new())
            .await
            .unwrap();

        sender.send(Ok(None)).await.unwrap();

        let mut sender = entry
            .begin_write(
                1000000,
                10,
                "text/html".to_string(),
                Labels::from_iter(vec![("a".to_string(), "b".to_string())]),
            )
            .await
            .unwrap();
        sender
            .send(Ok(Some(Bytes::from(vec![0; 10]))))
            .await
            .unwrap();

        let record = entry
            .block_manager
            .write()
            .unwrap()
            .load_block(1000000)
            .unwrap()
            .read()
            .unwrap()
            .get_record(1000000)
            .unwrap()
            .clone();
        assert_eq!(record.content_type, "text/html");
        assert_eq!(record.labels.len(), 1);
        assert_eq!(record.labels[0].name, "a");
        assert_eq!(record.labels[0].value, "b");
    }

    #[rstest]
    #[serial]
    #[tokio::test]
    async fn test_begin_not_override_if_different_size(entry: Arc<Entry>) {
        let mut sender = entry
            .begin_write(1000000, 10, "text/plain".to_string(), Labels::new())
            .await
            .unwrap();
        sender.send(Ok(None)).await.unwrap();

        let err = entry
            .begin_write(
                1000000,
                5,
                "text/html".to_string(),
                Labels::from_iter(vec![("a".to_string(), "b".to_string())]),
            )
            .await
            .err();
        assert_eq!(
            err,
            Some(ReductError::conflict(
                "A record with timestamp 1000000 already exists"
            ))
        );
    }

    #[rstest]
    #[serial]
    #[tokio::test]
    async fn test_belated_record_readable_after_rotation(entry: Arc<Entry>) {
        // Fill the first block
        write_stub_record(&entry, 1000000).await;
        // Rotate to a new block
        write_stub_record(&entry, 3000000).await;
        // Belated write into the first block
        write_stub_record(&entry, 2000000).await;

        // We must be able to read the belated record back
        let reader = entry.begin_read(2000000).wait();
        assert!(reader.is_ok(), "Belated record should be readable");
    }
}
