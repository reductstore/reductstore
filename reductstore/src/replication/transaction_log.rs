// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::core::file_cache::FILE_CACHE;
use crate::replication::Transaction;
use log::{debug, error, warn};
use reduct_base::error::ReductError;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

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
    file_path: PathBuf,
    capacity_in_bytes: usize,
    write_pos: usize,
    read_pos: usize,
}

const HEADER_SIZE: usize = 16;
const ENTRY_SIZE: usize = 9;

impl Drop for TransactionLog {
    fn drop(&mut self) {
        // Flush and sync the file in a separate thread.
        let file = FILE_CACHE.write_or_create(&self.file_path, SeekFrom::Current(0));
        if file.is_err() {
            error!("Failed to open transaction log: {}", file.err().unwrap());
            return;
        }

        let file = file.unwrap().upgrade().unwrap();
        let sync = || {
            let mut file = file.write()?;
            file.flush()?;
            file.sync_all()?;

            Ok::<(), ReductError>(())
        };

        sync().map_or_else(|e| error!("Failed to sync transaction log: {}", e), |r| r);
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
    pub fn try_load_or_create(path: PathBuf, capacity: usize) -> Result<Self, ReductError> {
        let init_capacity_in_bytes = capacity * ENTRY_SIZE + HEADER_SIZE;

        let instance = if !path.try_exists()? {
            let file = FILE_CACHE
                .write_or_create(&path, SeekFrom::Current(0))?
                .upgrade()?;
            let mut file = file.write()?;

            file.set_len(init_capacity_in_bytes as u64)?;
            file.seek(SeekFrom::Start(0))?;
            file.write_all(HEADER_SIZE.to_be_bytes().as_ref())?;
            file.write_all(HEADER_SIZE.to_be_bytes().as_ref())?;
            file.sync_all()?;

            Self {
                file_path: path,
                capacity_in_bytes: init_capacity_in_bytes,
                write_pos: HEADER_SIZE,
                read_pos: HEADER_SIZE,
            }
        } else {
            let file = FILE_CACHE.read(&path, SeekFrom::Start(0))?.upgrade()?;
            let mut file = file.write()?;

            let mut buf = [0u8; 16];
            file.read_exact(&mut buf)?;
            let write_pos = u64::from_be_bytes(buf[0..8].try_into().unwrap()) as usize;
            let read_pos = u64::from_be_bytes(buf[8..16].try_into().unwrap()) as usize;
            let capacity_in_bytes = file.metadata()?.len() as usize;
            let capacity_in_bytes = if init_capacity_in_bytes != capacity_in_bytes {
                // If the capacity is changed, we need to check if the log is empty
                // then we can change the capacity.
                if read_pos == write_pos {
                    debug!(
                        "Transaction log {:?}' size changed from {} to {} bytes",
                        path, capacity_in_bytes, init_capacity_in_bytes
                    );
                    file.set_len(init_capacity_in_bytes as u64)?;
                    init_capacity_in_bytes
                } else {
                    warn!("Cannot change the capacity of the transaction log {:?} from {} to {} bytes because it is not empty", path, capacity_in_bytes, init_capacity_in_bytes);
                    capacity_in_bytes
                }
            } else {
                capacity_in_bytes
            };

            Self {
                file_path: path,
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
    pub fn push_back(
        &mut self,
        transaction: Transaction,
    ) -> Result<Option<Transaction>, ReductError> {
        {
            let file = FILE_CACHE
                .write_or_create(&self.file_path, SeekFrom::Start(self.write_pos as u64))?
                .upgrade()?;
            let mut file = file.write()?;

            let mut buf = [0u8; ENTRY_SIZE];
            buf[0] = transaction.clone().into();
            buf[1..9].copy_from_slice(&transaction.timestamp().to_be_bytes());

            file.write_all(&buf)?;
            self.write_pos += ENTRY_SIZE;

            if self.write_pos >= self.capacity_in_bytes {
                self.write_pos = HEADER_SIZE;
            }

            file.seek(SeekFrom::Start(0))?;
            file.write_all(&self.write_pos.to_be_bytes())?;
        }

        if self.write_pos == self.read_pos {
            let transaction = self.unsafe_head(1)?.get(0).cloned();

            self.unsafe_pop()?;
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

    pub fn front(&self, n: usize) -> Result<Vec<Transaction>, ReductError> {
        if self.is_empty() {
            return Ok(Vec::new());
        }
        let transaction = self.unsafe_head(n)?;
        Ok(transaction)
    }

    pub fn pop_front(&mut self, n: usize) -> Result<usize, ReductError> {
        let mut popped = 0usize;
        for _ in 0..n {
            if self.read_pos == self.write_pos {
                break;
            }
            self.unsafe_pop()?;
            popped += 1;
        }

        Ok(popped)
    }

    fn unsafe_head(&self, n: usize) -> Result<Vec<Transaction>, ReductError> {
        let mut buf = [0u8; ENTRY_SIZE];
        let mut read_pos = self.read_pos;
        let mut transactions = Vec::with_capacity(n);
        let file = FILE_CACHE
            .read(&self.file_path, SeekFrom::Start(read_pos as u64))?
            .upgrade()?;
        let mut file = file.write()?;

        for _ in 0..n {
            file.seek(SeekFrom::Start(read_pos as u64))?;
            file.read_exact(&mut buf)?;
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

    fn unsafe_pop(&mut self) -> Result<(), ReductError> {
        self.read_pos += ENTRY_SIZE;

        if self.read_pos >= self.capacity_in_bytes {
            self.read_pos = HEADER_SIZE;
        }

        {
            let file = FILE_CACHE
                .write_or_create(&self.file_path, SeekFrom::Start(8))?
                .upgrade()?;
            let mut file = file.write()?;
            file.write_all(&self.read_pos.to_be_bytes())?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;
    use std::fs;
    use tempfile::tempdir;

    #[rstest]
    fn test_new_transaction_log(path: PathBuf) {
        let transaction_log = TransactionLog::try_load_or_create(path, 100).unwrap();
        assert_eq!(transaction_log.is_empty(), true);
    }

    #[rstest]
    fn test_write_read_transaction_log(path: PathBuf) {
        let mut transaction_log = TransactionLog::try_load_or_create(path, 100).unwrap();
        assert_eq!(
            transaction_log
                .push_back(Transaction::WriteRecord(1))
                .unwrap(),
            None
        );

        assert_eq!(transaction_log.len(), 1);

        assert_eq!(
            transaction_log
                .push_back(Transaction::WriteRecord(2))
                .unwrap(),
            None
        );
        assert_eq!(transaction_log.len(), 2);
        assert_eq!(transaction_log.is_empty(), false);
        assert_eq!(
            transaction_log.front(2).unwrap(),
            vec![Transaction::WriteRecord(1), Transaction::WriteRecord(2),]
        );

        assert_eq!(transaction_log.pop_front(2).unwrap(), 2);
        assert_eq!(transaction_log.is_empty(), true);

        assert_eq!(transaction_log.pop_front(1).unwrap(), 0);
    }

    #[rstest]
    fn test_out_of_range(path: PathBuf) {
        let mut transaction_log = TransactionLog::try_load_or_create(path, 100).unwrap();
        assert_eq!(
            transaction_log
                .push_back(Transaction::WriteRecord(1))
                .unwrap(),
            None
        );
        assert_eq!(
            transaction_log
                .push_back(Transaction::WriteRecord(2))
                .unwrap(),
            None
        );
        assert_eq!(transaction_log.len(), 2);
        assert_eq!(transaction_log.is_empty(), false);

        assert_eq!(
            transaction_log.front(3).unwrap(),
            vec![Transaction::WriteRecord(1), Transaction::WriteRecord(2),],
            "We return only the available transactions."
        );

        assert_eq!(
            transaction_log.pop_front(3).unwrap(),
            2,
            "We pop only the available transactions."
        );
        assert_eq!(transaction_log.is_empty(), true);
    }

    #[rstest]
    fn test_overflow(path: PathBuf) {
        let mut transaction_log = TransactionLog::try_load_or_create(path, 3).unwrap();
        for i in 1..5 {
            transaction_log
                .push_back(Transaction::WriteRecord(i))
                .unwrap();
        }

        assert_eq!(transaction_log.len(), 2);
        assert_eq!(
            transaction_log.front(2).unwrap(),
            vec![Transaction::WriteRecord(3), Transaction::WriteRecord(4),]
        );
    }

    #[rstest]
    fn test_recovery(path: PathBuf) {
        let mut transaction_log = TransactionLog::try_load_or_create(path.clone(), 3).unwrap();
        for i in 1..5 {
            transaction_log
                .push_back(Transaction::WriteRecord(i))
                .unwrap();
        }

        let mut transaction_log = TransactionLog::try_load_or_create(path, 3).unwrap();
        assert_eq!(transaction_log.len(), 2);
        assert_eq!(
            transaction_log.front(2).unwrap(),
            vec![Transaction::WriteRecord(3), Transaction::WriteRecord(4),]
        );

        assert_eq!(transaction_log.pop_front(2).unwrap(), 2);
        assert_eq!(transaction_log.is_empty(), true);
    }

    #[rstest]
    fn test_recovery_init(path: PathBuf) {
        let mut transaction_log = TransactionLog::try_load_or_create(path.clone(), 3).unwrap();
        transaction_log
            .push_back(Transaction::WriteRecord(1))
            .unwrap();
        drop(transaction_log);

        let transaction_log = TransactionLog::try_load_or_create(path, 3).unwrap();
        assert_eq!(transaction_log.write_pos, HEADER_SIZE + ENTRY_SIZE);
        assert_eq!(transaction_log.read_pos, HEADER_SIZE);
    }

    #[rstest]
    fn test_recovery_empty_cache(path: PathBuf) {
        let mut transaction_log = TransactionLog::try_load_or_create(path.clone(), 3).unwrap();
        transaction_log
            .push_back(Transaction::WriteRecord(1))
            .unwrap();

        FILE_CACHE.discard_recursive(&path).unwrap(); // discard the cache to simulate restart

        let mut transaction_log = TransactionLog::try_load_or_create(path, 3).unwrap();

        // check if the transaction log is still working after cache discard
        assert_eq!(
            transaction_log.front(1).unwrap(),
            vec![Transaction::WriteRecord(1)]
        );
        assert_eq!(transaction_log.pop_front(1).unwrap(), 1);
        assert!(transaction_log.is_empty());
    }

    #[rstest]
    fn test_resize_empty_log(path: PathBuf) {
        TransactionLog::try_load_or_create(path.clone(), 3).unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().len() as usize,
            ENTRY_SIZE * 3 + HEADER_SIZE
        );

        TransactionLog::try_load_or_create(path.clone(), 5).unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().len() as usize,
            ENTRY_SIZE * 5 + HEADER_SIZE
        );
    }

    #[rstest]
    fn test_resize_non_empty_log(path: PathBuf) {
        let mut transaction_log = TransactionLog::try_load_or_create(path.clone(), 3).unwrap();
        transaction_log
            .push_back(Transaction::WriteRecord(1))
            .unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().len() as usize,
            ENTRY_SIZE * 3 + HEADER_SIZE
        );

        TransactionLog::try_load_or_create(path.clone(), 5).unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().len() as usize,
            ENTRY_SIZE * 3 + HEADER_SIZE,
            "The log is not empty, so the capacity should not be changed."
        );
    }

    #[fixture]
    fn path() -> PathBuf {
        let path = tempdir().unwrap().into_path().join("transaction_log");
        path
    }
}
