// Copyright 2024 ReductSoftware UG
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
    pub(crate) fn begin_read(&self, time: u64) -> Result<RecordReader, ReductError> {
        debug!("Reading record for ts={}", time);

        let (block_ref, record) = {
            let bm = self.block_manager.read()?;
            let block_ref = bm.find_block(time)?;
            let block = block_ref.read()?;
            let record = block
                .get_record(time)
                .ok_or_else(|| not_found!("No record with timestamp {}", time))?
                .clone();
            (block_ref.clone(), record)
        };

        if record.state == record::State::Started as i32 {
            return Err(too_early!(
                "Record with timestamp {} is still being written",
                time
            ));
        }

        if record.state == record::State::Errored as i32 {
            return Err(internal_server_error!(
                "Record with timestamp {} is broken",
                time
            ));
        }

        RecordReader::try_new(self.block_manager.clone(), block_ref, time, true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::entry::tests::{entry, path, write_record, write_stub_record};
    use crate::storage::entry::EntrySettings;
    use crate::storage::storage::MAX_IO_BUFFER_SIZE;
    use bytes::Bytes;
    use reduct_base::error::ReductError;
    use reduct_base::Labels;
    use rstest::rstest;
    use std::path::PathBuf;

    #[rstest]
    #[tokio::test]
    async fn test_begin_read_empty(entry: Entry) {
        let writer = entry.begin_read(1000).await;
        assert_eq!(
            writer.err(),
            Some(not_found!("No record with timestamp 1000"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_begin_read_early(mut entry: Entry) {
        write_stub_record(&mut entry, 1000000).await.unwrap();
        let writer = entry.begin_read(1000).await;
        assert_eq!(
            writer.err(),
            Some(not_found!("No record with timestamp 1000"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_begin_read_late(mut entry: Entry) {
        write_stub_record(&mut entry, 1000000).await.unwrap();
        let reader = entry.begin_read(2000000).await;
        assert_eq!(
            reader.err(),
            Some(not_found!("No record with timestamp 2000000"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_begin_read_broken(entry: Entry) {
        let sender = entry
            .begin_write(1000000, 10, "text/plain".to_string(), Labels::new())
            .await
            .unwrap();
        sender
            .tx()
            .send(Ok(Some(Bytes::from(vec![0; 50]))))
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let reader = entry.begin_read(1000000).await;
        assert_eq!(
            reader.err(),
            Some(internal_server_error!(
                "Record with timestamp 1000000 is broken"
            ))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_begin_read_still_written(entry: Entry) {
        let sender = entry
            .begin_write(1000000, 10, "text/plain".to_string(), Labels::new())
            .await
            .unwrap();
        sender
            .tx()
            .send(Ok(Some(Bytes::from(vec![0; 5]))))
            .await
            .unwrap();

        let reader = entry.begin_read(1000000).await;
        assert_eq!(
            reader.err(),
            Some(too_early!(
                "Record with timestamp 1000000 is still being written"
            ))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_begin_read_not_found(mut entry: Entry) {
        write_stub_record(&mut entry, 1000000).await.unwrap();
        write_stub_record(&mut entry, 3000000).await.unwrap();

        let reader = entry.begin_read(2000000).await;
        assert_eq!(
            reader.err(),
            Some(not_found!("No record with timestamp 2000000"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_begin_read_ok1(mut entry: Entry) {
        write_stub_record(&mut entry, 1000000).await.unwrap();
        let mut reader = entry.begin_read(1000000).await.unwrap();
        assert_eq!(
            reader.rx().recv().await.unwrap(),
            Ok(Bytes::from("0123456789"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_begin_read_ok2(mut entry: Entry) {
        write_stub_record(&mut entry, 1000000).await.unwrap();
        write_stub_record(&mut entry, 1010000).await.unwrap();

        let mut reader = entry.begin_read(1010000).await.unwrap();
        assert_eq!(
            reader.rx().recv().await.unwrap(),
            Ok(Bytes::from("0123456789"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_begin_read_ok_in_chunks(mut entry: Entry) {
        let mut data = vec![0; MAX_IO_BUFFER_SIZE + 1];
        data[0] = 1;
        data[MAX_IO_BUFFER_SIZE] = 2;

        write_record(&mut entry, 1000000, data.clone())
            .await
            .unwrap();

        let mut reader = entry.begin_read(1000000).await.unwrap();
        assert_eq!(
            reader.rx().recv().await.unwrap().unwrap().to_vec(),
            data[0..MAX_IO_BUFFER_SIZE]
        );
        assert_eq!(
            reader.rx().recv().await.unwrap().unwrap().to_vec(),
            data[MAX_IO_BUFFER_SIZE..]
        );
        assert_eq!(reader.rx().recv().await, None);
    }

    #[rstest]
    #[tokio::test]
    async fn test_search(path: PathBuf) {
        let mut entry = entry(
            EntrySettings {
                max_block_size: 10000,
                max_block_records: 5,
            },
            path,
        );

        let step = 100000;
        for i in 0..100 {
            write_stub_record(&mut entry, i * step).await.unwrap();
        }

        let reader = entry.begin_read(30 * step).await.unwrap();
        assert_eq!(reader.timestamp(), 3000000);
    }
}
