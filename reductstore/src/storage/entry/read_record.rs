// Copyright 2024-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::storage::entry::{Entry, RecordReader};
use crate::storage::proto::record;
use log::debug;
use reduct_base::error::ReductError;
use reduct_base::{internal_server_error, not_found, too_early};

impl Entry {
    /// Starts a new record read.
    ///
    /// # Arguments
    ///
    /// * `time` - The timestamp of the record.
    ///
    /// # Returns
    ///
    /// * `RecordReader` - The record reader to read the record content in chunks.
    /// * `HTTPError` - The error if any.
    pub(crate) async fn begin_read(&self, time: u64) -> Result<RecordReader, ReductError> {
        debug!(
            "Reading record for ts={} in {}/{}",
            time, self.bucket_name, self.name
        );

        let (block_ref, record) = {
            let mut bm = self.block_manager.write().await?;
            let block_ref = bm.find_block(time)?;
            let block = block_ref.read()?;
            let record = block
                .get_record(time)
                .ok_or_else(|| {
                    not_found!(
                        "Record {} not found in block {}/{}/{}",
                        time,
                        self.bucket_name,
                        self.name,
                        block.block_id(),
                    )
                })?
                .clone();
            (block_ref.clone(), record)
        };

        if record.state == record::State::Started as i32 {
            return Err(too_early!(
                "Record with timestamp {} in {}/{} is still being written",
                time,
                self.bucket_name,
                self.name
            ));
        }

        if record.state == record::State::Errored as i32 {
            return Err(internal_server_error!(
                "Record with timestamp {} in {}/{} is broken",
                time,
                self.bucket_name,
                self.name
            ));
        }

        RecordReader::try_new(self.block_manager.clone(), block_ref, time, None).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::MAX_IO_BUFFER_SIZE;
    use crate::storage::entry::tests::{entry, path, write_record, write_stub_record};
    use crate::storage::entry::EntrySettings;
    use bytes::Bytes;
    use reduct_base::error::ReductError;
    use reduct_base::io::ReadRecord;
    use reduct_base::Labels;
    use rstest::rstest;
    use std::path::PathBuf;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_begin_read_empty(entry: Arc<Entry>) {
        let writer = entry.begin_read(1000).await;
        assert_eq!(
            writer.err(),
            Some(not_found!("No record with timestamp 1000"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_begin_read_early(entry: Arc<Entry>) {
        write_stub_record(&entry, 1000000).await;
        let writer = entry.begin_read(1000).await;
        assert_eq!(
            writer.err(),
            Some(not_found!("No record with timestamp 1000"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_begin_read_late(entry: Arc<Entry>) {
        write_stub_record(&entry, 1000000).await;
        let reader = entry.begin_read(2000000).await;
        assert_eq!(
            reader.err(),
            Some(not_found!(
                "Record 2000000 not found in block bucket/entry/1000000"
            ))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_begin_read_broken(entry: Arc<Entry>) {
        let mut sender = entry
            .clone()
            .begin_write(1000000, 10, "text/plain".to_string(), Labels::new())
            .await
            .unwrap();
        sender
            .send(Ok(Some(Bytes::from(vec![0; 50]))))
            .await
            .unwrap();
        sender.send(Ok(None)).await.unwrap();

        let reader = entry.begin_read(1000000).await;
        assert_eq!(
            reader.err(),
            Some(internal_server_error!(
                "Record with timestamp 1000000 in bucket/entry is broken"
            ))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_begin_read_still_written(entry: Arc<Entry>) {
        let mut sender = entry
            .clone()
            .begin_write(1000000, 10, "text/plain".to_string(), Labels::new())
            .await
            .unwrap();
        sender
            .send(Ok(Some(Bytes::from(vec![0; 5]))))
            .await
            .unwrap();

        let reader = entry.begin_read(1000000).await;
        assert_eq!(
            reader.err(),
            Some(too_early!(
                "Record with timestamp 1000000 in bucket/entry is still being written"
            ))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_begin_read_not_found(entry: Arc<Entry>) {
        write_stub_record(&entry, 1000000).await;
        write_stub_record(&entry, 3000000).await;

        let reader = entry.begin_read(2000000).await;
        assert_eq!(
            reader.err(),
            Some(not_found!(
                "Record 2000000 not found in block bucket/entry/1000000"
            ))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_begin_read_ok1(entry: Arc<Entry>) {
        write_stub_record(&entry, 1000000).await;
        let mut reader = entry.begin_read(1000000).await.unwrap();
        assert_eq!(reader.read_chunk().unwrap(), Ok(Bytes::from("0123456789")));
    }

    #[rstest]
    #[tokio::test]
    async fn test_begin_read_ok2(entry: Arc<Entry>) {
        write_stub_record(&entry, 1000000).await;
        write_stub_record(&entry, 1010000).await;

        let mut reader = entry.begin_read(1010000).await.unwrap();
        assert_eq!(reader.read_chunk().unwrap(), Ok(Bytes::from("0123456789")));
    }

    #[rstest]
    #[tokio::test]
    async fn test_begin_read_ok_in_chunks(entry: Arc<Entry>) {
        let mut data = vec![0; MAX_IO_BUFFER_SIZE + 1];
        data[0] = 1;
        data[MAX_IO_BUFFER_SIZE] = 2;

        write_record(&entry, 1000000, data.clone()).await;

        let mut reader = entry.begin_read(1000000).await.unwrap();
        assert_eq!(
            reader.read_chunk().unwrap().unwrap().to_vec(),
            data[0..MAX_IO_BUFFER_SIZE]
        );
        assert_eq!(
            reader.read_chunk().unwrap().unwrap().to_vec(),
            data[MAX_IO_BUFFER_SIZE..]
        );
        assert_eq!(reader.read_chunk(), None);
    }

    #[rstest]
    #[tokio::test]
    async fn test_search(path: PathBuf) {
        let entry = entry(
            EntrySettings {
                max_block_size: 10000,
                max_block_records: 5,
            },
            path,
        );

        let step = 100000;
        for i in 0..10 {
            write_stub_record(&entry, i * step).await;
        }

        let reader = entry.begin_read(5 * step).await.unwrap();
        assert_eq!(reader.meta().timestamp(), 500000);
    }
}
