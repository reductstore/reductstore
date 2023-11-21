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
        self.file.write_u8(transaction.clone().into()).await?;
        self.file.write_u64(*transaction.timestamp()).await?;
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

    pub async fn read(&mut self) -> Result<Option<Transaction>, ReductError> {
        if self.read_pos == self.write_pos {
            return Ok(None);
        }

        self.file
            .seek(SeekFrom::Start(self.read_pos as u64))
            .await?;
        let transaction_type = self.file.read_u8().await?;
        let timestamp = self.file.read_u64().await?;
        self.read_pos += TRANSACTION_LOG_ENTRY_SIZE;

        if self.read_pos >= self.capacity_in_bytes {
            self.read_pos = TRANSACTION_LOG_HEADER_SIZE;
        }

        Ok(Some(Transaction::try_from(transaction_type)?))
    }
}
