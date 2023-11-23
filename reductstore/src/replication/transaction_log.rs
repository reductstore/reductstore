// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::replication::Transaction;
use reduct_base::error::ReductError;
use std::io::SeekFrom;
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

/// Transaction log for replication.
///
/// Format
///
/// | 8 byte - write position | 8 byte - read position |
/// | byte - transaction type 0 | 8 byte - timestamp 0 |
///  .........
/// | byte - transaction type n | 8 byte - timestamp n |
///
struct TransactionLog {
    file: File,
    capacity_in_bytes: usize,
    write_pos: usize,
    read_pos: usize,
}

const TRANSACTION_LOG_HEADER_SIZE: usize = 16;
const TRANSACTION_LOG_ENTRY_SIZE: usize = 9;

#[derive(Debug, PartialEq)]
enum WriteStatus {
    Ok,
    Overflow,
}

impl TransactionLog {
    pub async fn try_load(path: PathBuf, capacity: usize) -> Result<Self, ReductError> {
        let instance = if !path.exists() {
            let file = File::create(path).await?;
            Self {
                file,
                capacity_in_bytes: capacity * TRANSACTION_LOG_ENTRY_SIZE
                    + TRANSACTION_LOG_HEADER_SIZE,
                write_pos: TRANSACTION_LOG_HEADER_SIZE,
                read_pos: TRANSACTION_LOG_HEADER_SIZE,
            }
        } else {
            let mut file = OpenOptions::new().read(true).write(true).open(path).await?;
            file.seek(SeekFrom::Start(0)).await?;

            let mut buf = [0u8; 16];
            file.read_exact(&mut buf).await?;
            let write_pos = u64::from_be_bytes(buf[0..8].try_into().unwrap()) as usize;
            let read_pos = u64::from_be_bytes(buf[8..16].try_into().unwrap()) as usize;
            Self {
                file,
                capacity_in_bytes: capacity,
                write_pos,
                read_pos,
            }
        };

        Ok(instance)
    }

    pub async fn write(&mut self, transaction: Transaction) -> Result<WriteStatus, ReductError> {
        self.file
            .seek(SeekFrom::Start(self.write_pos as u64))
            .await?;

        let mut buf = [0u8; TRANSACTION_LOG_ENTRY_SIZE];
        buf[0] = transaction.clone().into();
        buf[1..9].copy_from_slice(&transaction.timestamp().to_be_bytes());
        self.file.write_all(&buf).await?;
        self.write_pos += TRANSACTION_LOG_ENTRY_SIZE;

        if self.write_pos >= self.capacity_in_bytes {
            self.write_pos = TRANSACTION_LOG_HEADER_SIZE;
        }

        if self.write_pos == self.read_pos {
            self.read_pos += TRANSACTION_LOG_ENTRY_SIZE;
            if self.read_pos >= self.capacity_in_bytes {
                self.read_pos = TRANSACTION_LOG_HEADER_SIZE;
            }
            return Ok(WriteStatus::Overflow);
        }

        Ok(WriteStatus::Ok)
    }

    pub fn is_empty(&self) -> bool {
        self.read_pos == self.write_pos
    }

    pub fn size(&self) -> usize {
        if self.write_pos >= self.read_pos {
            self.write_pos - self.read_pos
        } else {
            self.capacity_in_bytes - self.read_pos + self.write_pos
        }
    }

    pub async fn head(&mut self) -> Result<Option<Transaction>, ReductError> {
        if self.is_empty() {
            return Ok(None);
        }

        self.file
            .seek(SeekFrom::Start(self.read_pos as u64))
            .await?;
        let mut buf = [0u8; TRANSACTION_LOG_ENTRY_SIZE];
        self.file.read_exact(&mut buf).await?;
        let transaction_type = buf[0];
        let timestamp = u64::from_be_bytes(buf[1..9].try_into().unwrap());
        self.read_pos += TRANSACTION_LOG_ENTRY_SIZE;

        match transaction_type {
            0 => Ok(Some(Transaction::WriteRecord(timestamp))),
            _ => Err(ReductError::internal_server_error(
                "Invalid transaction type",
            )),
        }
    }

    pub async fn pop(&mut self) -> Result<(), ReductError> {
        if self.read_pos == self.write_pos {
            return Ok(());
        }
        self.read_pos += TRANSACTION_LOG_ENTRY_SIZE;

        if self.read_pos >= self.capacity_in_bytes {
            self.read_pos = TRANSACTION_LOG_HEADER_SIZE;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;
    use tempfile::tempdir;

    #[rstest]
    #[tokio::test]
    async fn test_new_transaction_log(path: PathBuf) {
        let transaction_log = TransactionLog::try_load(path, 100).await.unwrap();
        assert_eq!(transaction_log.is_empty(), true);
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_transaction_log(path: PathBuf) {
        let mut transaction_log = TransactionLog::try_load(path, 100).await.unwrap();
        let transaction = Transaction::WriteRecord(1);
        let status = transaction_log.write(transaction.clone()).await.unwrap();
        assert_eq!(status, WriteStatus::Ok);
        assert_eq!(transaction_log.is_empty(), false);
        assert_eq!(transaction_log.size(), 9);
        assert_eq!(transaction_log.head().await.unwrap(), Some(transaction));
    }

    #[fixture]
    fn path() -> PathBuf {
        let path = tempdir().unwrap().into_path().join("transaction_log");
        path
    }
}
