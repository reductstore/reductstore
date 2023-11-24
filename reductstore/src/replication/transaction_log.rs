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
pub(super) struct TransactionLog {
    file: File,
    capacity_in_bytes: usize,
    write_pos: usize,
    read_pos: usize,
}

const HEADER_SIZE: usize = 16;
const ENTRY_SIZE: usize = 9;

impl TransactionLog {
    /// Create a new transaction log or load an existing one.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the transaction log file.
    /// * `capacity` - Maximum number of transactions to store. Ignored if the file already exists.
    ///
    /// # Returns
    ///
    /// A new transaction log instance or an error.
    pub async fn try_load_or_create(path: PathBuf, capacity: usize) -> Result<Self, ReductError> {
        let instance = if !path.exists() {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)
                .await?;

            let capacity_in_bytes = capacity * ENTRY_SIZE + HEADER_SIZE;
            file.set_len(capacity_in_bytes as u64).await?;
            Self {
                file,
                capacity_in_bytes,
                write_pos: HEADER_SIZE,
                read_pos: HEADER_SIZE,
            }
        } else {
            let mut file = OpenOptions::new().read(true).write(true).open(path).await?;
            file.seek(SeekFrom::Start(0)).await?;

            let mut buf = [0u8; 16];
            file.read_exact(&mut buf).await?;
            let write_pos = u64::from_be_bytes(buf[0..8].try_into().unwrap()) as usize;
            let read_pos = u64::from_be_bytes(buf[8..16].try_into().unwrap()) as usize;
            let capacity_in_bytes = file.metadata().await?.len() as usize;
            Self {
                file,
                capacity_in_bytes,
                write_pos,
                read_pos,
            }
        };

        Ok(instance)
    }

    /// Push a new transaction to the log.
    ///
    /// # Arguments
    ///
    /// * `transaction` - Transaction to push.
    ///
    /// # Returns
    ///
    /// The oldest transaction if the log is full, otherwise `None`.
    pub async fn push(
        &mut self,
        transaction: Transaction,
    ) -> Result<Option<Transaction>, ReductError> {
        self.file
            .seek(SeekFrom::Start(self.write_pos as u64))
            .await?;

        let mut buf = [0u8; ENTRY_SIZE];
        buf[0] = transaction.clone().into();
        buf[1..9].copy_from_slice(&transaction.timestamp().to_be_bytes());
        self.file.write_all(&buf).await?;
        self.write_pos += ENTRY_SIZE;

        if self.write_pos >= self.capacity_in_bytes {
            self.write_pos = HEADER_SIZE;
        }

        self.file.seek(SeekFrom::Start(0)).await?;
        self.file.write_all(&self.write_pos.to_be_bytes()).await?;

        if self.write_pos == self.read_pos {
            let transaction = self.unsafe_head().await?;
            self.unsafe_pop().await?;
            Ok(transaction)
        } else {
            Ok(None)
        }
    }

    pub fn is_empty(&self) -> bool {
        self.read_pos == self.write_pos
    }

    pub async fn head(&mut self) -> Result<Option<Transaction>, ReductError> {
        if self.is_empty() {
            return Ok(None);
        }
        self.unsafe_head().await
    }

    pub async fn pop(&mut self) -> Result<(), ReductError> {
        if self.read_pos == self.write_pos {
            return Err(ReductError::internal_server_error(
                "Transaction log is empty",
            ));
        }

        self.unsafe_pop().await
    }

    async fn unsafe_head(&mut self) -> Result<Option<Transaction>, ReductError> {
        self.file
            .seek(SeekFrom::Start(self.read_pos as u64))
            .await?;
        let mut buf = [0u8; ENTRY_SIZE];
        self.file.read_exact(&mut buf).await?;
        let transaction_type = buf[0];
        let timestamp = u64::from_be_bytes(buf[1..9].try_into().unwrap());

        match transaction_type {
            0 => Ok(Some(Transaction::WriteRecord(timestamp))),
            _ => Err(ReductError::internal_server_error(
                "Invalid transaction type",
            )),
        }
    }

    async fn unsafe_pop(&mut self) -> Result<(), ReductError> {
        self.read_pos += ENTRY_SIZE;

        if self.read_pos >= self.capacity_in_bytes {
            self.read_pos = HEADER_SIZE;
        }

        self.file.seek(SeekFrom::Start(8)).await?;
        self.file.write_all(&self.read_pos.to_be_bytes()).await?;

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
        let transaction_log = TransactionLog::try_load_or_create(path, 100).await.unwrap();
        assert_eq!(transaction_log.is_empty(), true);
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_read_transaction_log(path: PathBuf) {
        let mut transaction_log = TransactionLog::try_load_or_create(path, 100).await.unwrap();
        assert_eq!(
            transaction_log
                .push(Transaction::WriteRecord(1))
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            transaction_log
                .push(Transaction::WriteRecord(2))
                .await
                .unwrap(),
            None
        );
        assert_eq!(transaction_log.is_empty(), false);
        assert_eq!(
            transaction_log.head().await.unwrap(),
            Some(Transaction::WriteRecord(1))
        );

        transaction_log.pop().await.unwrap();
        assert_eq!(
            transaction_log.head().await.unwrap(),
            Some(Transaction::WriteRecord(2))
        );
        assert_eq!(transaction_log.is_empty(), false);

        transaction_log.pop().await.unwrap();
        assert_eq!(transaction_log.is_empty(), true);

        let err = transaction_log.pop().await.err().unwrap();
        assert_eq!(
            err,
            ReductError::internal_server_error("Transaction log is empty")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_overflow(path: PathBuf) {
        let mut transaction_log = TransactionLog::try_load_or_create(path, 3).await.unwrap();
        for i in 1..5 {
            transaction_log
                .push(Transaction::WriteRecord(i))
                .await
                .unwrap();
        }

        assert_eq!(
            transaction_log.head().await.unwrap(),
            Some(Transaction::WriteRecord(3))
        );
        transaction_log.pop().await.unwrap();

        assert_eq!(
            transaction_log.head().await.unwrap(),
            Some(Transaction::WriteRecord(4))
        );
        transaction_log.pop().await.unwrap();

        assert_eq!(transaction_log.is_empty(), true);
    }

    #[rstest]
    #[tokio::test]
    async fn test_recovery(path: PathBuf) {
        let mut transaction_log = TransactionLog::try_load_or_create(path.clone(), 3)
            .await
            .unwrap();
        for i in 1..5 {
            transaction_log
                .push(Transaction::WriteRecord(i))
                .await
                .unwrap();
        }

        let mut transaction_log = TransactionLog::try_load_or_create(path, 3).await.unwrap();
        assert_eq!(
            transaction_log.head().await.unwrap(),
            Some(Transaction::WriteRecord(3))
        );
        transaction_log.pop().await.unwrap();

        assert_eq!(
            transaction_log.head().await.unwrap(),
            Some(Transaction::WriteRecord(4))
        );
        transaction_log.pop().await.unwrap();

        assert_eq!(transaction_log.is_empty(), true);
    }

    #[fixture]
    fn path() -> PathBuf {
        let path = tempdir().unwrap().into_path().join("transaction_log");
        path
    }
}
