// Copyright 2023 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::replication::Transaction;
use reduct_base::error::ReductError;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::spawn;
use tokio::sync::RwLock;

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
    file: Arc<RwLock<File>>,
    capacity_in_bytes: usize,
    write_pos: usize,
    read_pos: usize,
}

const HEADER_SIZE: usize = 16;
const ENTRY_SIZE: usize = 9;

impl Drop for TransactionLog {
    fn drop(&mut self) {
        // Flush and sync the file in a separate thread.
        let file = Arc::clone(&self.file);
        let _ = spawn(async move {
            let mut file = file.write().await;
            file.flush().await?;
            file.sync_all().await?;

            Ok::<(), ReductError>(())
        });
    }
}

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
        let instance = if !path.try_exists()? {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)
                .await?;

            let capacity_in_bytes = capacity * ENTRY_SIZE + HEADER_SIZE;
            file.set_len(capacity_in_bytes as u64).await?;
            file.seek(SeekFrom::Start(0)).await?;
            file.write_all(HEADER_SIZE.to_be_bytes().as_ref()).await?;
            file.write_all(HEADER_SIZE.to_be_bytes().as_ref()).await?;
            file.sync_all().await?;

            Self {
                file: Arc::new(RwLock::from(file)),
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
                file: Arc::new(RwLock::from(file)),
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
    pub async fn push_back(
        &mut self,
        transaction: Transaction,
    ) -> Result<Option<Transaction>, ReductError> {
        {
            let mut file = self.file.write().await;

            file.seek(SeekFrom::Start(self.write_pos as u64)).await?;

            let mut buf = [0u8; ENTRY_SIZE];
            buf[0] = transaction.clone().into();
            buf[1..9].copy_from_slice(&transaction.timestamp().to_be_bytes());

            file.write_all(&buf).await?;
            self.write_pos += ENTRY_SIZE;

            if self.write_pos >= self.capacity_in_bytes {
                self.write_pos = HEADER_SIZE;
            }

            file.seek(SeekFrom::Start(0)).await?;
            file.write_all(&self.write_pos.to_be_bytes()).await?;
        }

        if self.write_pos == self.read_pos {
            let transaction = self.unsafe_head(1).await?.get(0).cloned();

            self.unsafe_pop().await?;
            Ok(transaction)
        } else {
            Ok(None)
        }
    }

    pub fn is_empty(&self) -> bool {
        self.read_pos == self.write_pos
    }

    pub fn len(&self) -> usize {
        let len_in_bytes = if self.read_pos <= self.write_pos {
            self.write_pos - self.read_pos
        } else {
            self.capacity_in_bytes
                .wrapping_sub(HEADER_SIZE)
                .wrapping_sub(self.read_pos)
                .wrapping_add(self.write_pos)
        };
        (len_in_bytes / ENTRY_SIZE) as usize
    }

    pub async fn front(&self, n: usize) -> Result<Vec<Transaction>, ReductError> {
        if self.is_empty() {
            return Ok(Vec::new());
        }
        let transaction = self.unsafe_head(n).await?;
        Ok(transaction)
    }

    pub async fn pop_front(&mut self, n: usize) -> Result<usize, ReductError> {
        let mut popped = 0usize;
        for _ in 0..n {
            if self.read_pos == self.write_pos {
                break;
            }
            self.unsafe_pop().await?;
            popped += 1;
        }

        Ok(popped)
    }

    async fn unsafe_head(&self, n: usize) -> Result<Vec<Transaction>, ReductError> {
        let mut buf = [0u8; ENTRY_SIZE];
        let mut read_pos = self.read_pos;
        let mut transactions = Vec::with_capacity(n);
        let mut file = self.file.write().await;

        for _ in 0..n {
            file.seek(SeekFrom::Start(read_pos as u64)).await?;
            file.read_exact(&mut buf).await?;
            let transaction_type = buf[0];
            let timestamp = u64::from_be_bytes(buf[1..9].try_into().unwrap());

            match transaction_type {
                0 => transactions.push(Transaction::WriteRecord(timestamp)),
                _ => {
                    return Err(ReductError::internal_server_error(
                        "Invalid transaction type",
                    ))
                }
            }

            read_pos += ENTRY_SIZE;
            if read_pos >= self.capacity_in_bytes {
                read_pos = HEADER_SIZE;
            }

            if read_pos == self.write_pos {
                break;
            }
        }

        Ok(transactions)
    }

    async fn unsafe_pop(&mut self) -> Result<(), ReductError> {
        self.read_pos += ENTRY_SIZE;

        if self.read_pos >= self.capacity_in_bytes {
            self.read_pos = HEADER_SIZE;
        }

        {
            let mut file = self.file.write().await;
            file.seek(SeekFrom::Start(8)).await?;
            file.write_all(&self.read_pos.to_be_bytes()).await?;
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
        let transaction_log = TransactionLog::try_load_or_create(path, 100).await.unwrap();
        assert_eq!(transaction_log.is_empty(), true);
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_read_transaction_log(path: PathBuf) {
        let mut transaction_log = TransactionLog::try_load_or_create(path, 100).await.unwrap();
        assert_eq!(
            transaction_log
                .push_back(Transaction::WriteRecord(1))
                .await
                .unwrap(),
            None
        );

        assert_eq!(transaction_log.len(), 1);

        assert_eq!(
            transaction_log
                .push_back(Transaction::WriteRecord(2))
                .await
                .unwrap(),
            None
        );
        assert_eq!(transaction_log.len(), 2);
        assert_eq!(transaction_log.is_empty(), false);
        assert_eq!(
            transaction_log.front(2).await.unwrap(),
            vec![Transaction::WriteRecord(1), Transaction::WriteRecord(2),]
        );

        assert_eq!(transaction_log.pop_front(2).await.unwrap(), 2);
        assert_eq!(transaction_log.is_empty(), true);

        assert_eq!(transaction_log.pop_front(1).await.unwrap(), 0);
    }

    #[rstest]
    #[tokio::test]
    async fn test_out_of_range(path: PathBuf) {
        let mut transaction_log = TransactionLog::try_load_or_create(path, 100).await.unwrap();
        assert_eq!(
            transaction_log
                .push_back(Transaction::WriteRecord(1))
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            transaction_log
                .push_back(Transaction::WriteRecord(2))
                .await
                .unwrap(),
            None
        );
        assert_eq!(transaction_log.len(), 2);
        assert_eq!(transaction_log.is_empty(), false);

        assert_eq!(
            transaction_log.front(3).await.unwrap(),
            vec![Transaction::WriteRecord(1), Transaction::WriteRecord(2),],
            "We return only the available transactions."
        );

        assert_eq!(
            transaction_log.pop_front(3).await.unwrap(),
            2,
            "We pop only the available transactions."
        );
        assert_eq!(transaction_log.is_empty(), true);
    }

    #[rstest]
    #[tokio::test]
    async fn test_overflow(path: PathBuf) {
        let mut transaction_log = TransactionLog::try_load_or_create(path, 3).await.unwrap();
        for i in 1..5 {
            transaction_log
                .push_back(Transaction::WriteRecord(i))
                .await
                .unwrap();
        }

        assert_eq!(transaction_log.len(), 2);
        assert_eq!(
            transaction_log.front(2).await.unwrap(),
            vec![Transaction::WriteRecord(3), Transaction::WriteRecord(4),]
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_recovery(path: PathBuf) {
        let mut transaction_log = TransactionLog::try_load_or_create(path.clone(), 3)
            .await
            .unwrap();
        for i in 1..5 {
            transaction_log
                .push_back(Transaction::WriteRecord(i))
                .await
                .unwrap();
        }

        let mut transaction_log = TransactionLog::try_load_or_create(path, 3).await.unwrap();
        assert_eq!(transaction_log.len(), 2);
        assert_eq!(
            transaction_log.front(2).await.unwrap(),
            vec![Transaction::WriteRecord(3), Transaction::WriteRecord(4),]
        );

        assert_eq!(transaction_log.pop_front(2).await.unwrap(), 2);
        assert_eq!(transaction_log.is_empty(), true);
    }

    #[rstest]
    #[tokio::test]
    async fn test_recovery_init(path: PathBuf) {
        let mut transaction_log = TransactionLog::try_load_or_create(path.clone(), 3)
            .await
            .unwrap();
        transaction_log
            .push_back(Transaction::WriteRecord(1))
            .await
            .unwrap();
        drop(transaction_log);

        let transaction_log = TransactionLog::try_load_or_create(path, 3).await.unwrap();
        assert_eq!(transaction_log.write_pos, HEADER_SIZE + ENTRY_SIZE);
        assert_eq!(transaction_log.read_pos, HEADER_SIZE);
    }

    #[fixture]
    fn path() -> PathBuf {
        let path = tempdir().unwrap().into_path().join("transaction_log");
        path
    }
}
