// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::collections::HashMap;
use std::io::ErrorKind::UnexpectedEof;
use std::io::{ErrorKind, SeekFrom};
use std::path::PathBuf;

use async_trait::async_trait;
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use reduct_base::error::ReductError;
use reduct_base::internal_server_error;

use crate::storage::file_cache::get_global_file_cache;
use crate::storage::proto::Record;

const WAL_FILE_SIZE: u64 = 1_000_000;

#[derive(PartialEq, Debug)]
pub(super) enum WalEntry {
    WriteRecord(Record),
    UpdateRecord(Record),
    RemoveBlock,
}

impl WalEntry {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            WalEntry::WriteRecord(record) => {
                let mut buf = Vec::new();
                buf.push(0);

                let record = record.encode_to_vec();
                buf.extend_from_slice(&(record.len() as u64).to_be_bytes());
                buf.extend_from_slice(&record);
                buf
            }
            WalEntry::UpdateRecord(record) => {
                let mut buf = Vec::new();
                buf.push(1);

                let record = record.encode_to_vec();
                buf.extend_from_slice(&(record.len() as u64).to_be_bytes());
                buf.extend_from_slice(&record);
                buf
            }
            WalEntry::RemoveBlock => {
                let mut buf = vec![2];
                buf.extend_from_slice(&0u64.to_be_bytes());
                buf
            }
        }
    }

    pub fn decode(type_code: u8, buf: &[u8]) -> Result<Self, ReductError> {
        match type_code {
            0 => {
                let record = Record::decode(buf).unwrap();
                Ok(WalEntry::WriteRecord(record))
            }
            1 => {
                let record = Record::decode(buf).unwrap();
                Ok(WalEntry::UpdateRecord(record))
            }
            2 => Ok(WalEntry::RemoveBlock),
            _ => Err(internal_server_error!("Invalid WAL entry")),
        }
    }
}

/// Manage WAL logs per block
#[async_trait]
pub(super) trait Wal {
    /// Append a WAL entry to the WAL file
    ///
    /// # Arguments
    ///
    /// * `block_id` - The block id to append the entry to
    /// * `entry` - The WAL entry to append
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the entry was successfully appended
    async fn append(&mut self, block_id: u64, entry: WalEntry) -> Result<(), ReductError>;

    /// Read all WAL entries for a block
    ///
    /// # Arguments
    ///
    /// * `block_id` - The block id to read the entries for
    ///
    /// # Returns
    ///
    /// * A vector of WAL entries
    async fn read(&self, block_id: u64) -> Result<Vec<WalEntry>, ReductError>;

    /// Remove the WAL file for a block
    async fn remove(&self, block_id: u64) -> Result<(), ReductError>;
}

struct WalImpl {
    root_path: PathBuf,
    path_map: HashMap<u64, PathBuf>,
}

impl WalImpl {
    pub fn new(path_buf: PathBuf) -> Self {
        // preallocate file
        WalImpl {
            root_path: path_buf,
            path_map: HashMap::new(),
        }
    }

    fn block_wal_path(&self, block_id: u64) -> PathBuf {
        self.root_path.join(format!("{}.wal", block_id))
    }
}

#[async_trait]
impl Wal for WalImpl {
    async fn append(&mut self, block_id: u64, entry: WalEntry) -> Result<(), ReductError> {
        let path = self.block_wal_path(block_id);
        let (file, created) = if !path.exists() {
            let file = get_global_file_cache().write_or_create(&path).await?;
            // preallocate file to speed up writes
            file.write().await.set_len(WAL_FILE_SIZE).await?;
            (file, true)
        } else {
            (get_global_file_cache().write_or_create(&path).await?, false)
        };

        if !created {
            // remove stop marker
            file.write().await.seek(SeekFrom::Current(-1)).await?;
        }

        let mut lock = file.write().await;
        lock.write_all(&entry.encode()).await?;

        // write stop marker
        lock.write_u8(255).await?;
        Ok(())
    }

    async fn read(&self, block_id: u64) -> Result<Vec<WalEntry>, ReductError> {
        let path = self.block_wal_path(block_id);
        let file = get_global_file_cache().read(&path).await?;
        let mut lock = file.write().await;
        lock.seek(SeekFrom::Start(0)).await?;

        let mut entries = Vec::new();
        loop {
            let entry_type = match lock.read_u8().await {
                Ok(t) => t,
                Err(err) => return Err(err.into()),
            };

            if entry_type == 255 {
                break;
            }

            let len = lock.read_u64().await?;
            let mut buf = vec![0; len as usize];
            lock.read_exact(&mut buf).await?;

            let entry = WalEntry::decode(entry_type, &buf)?;
            entries.push(entry);
        }

        Ok(entries)
    }

    async fn remove(&self, block_id: u64) -> Result<(), ReductError> {
        let path = self.block_wal_path(block_id);
        if path.exists() {
            get_global_file_cache().remove(&path).await?;
        }
        Ok(())
    }
}

/// Creates a new Write-Ahead Log (WAL) instance.
///
/// This function initializes a WAL directory at the specified path if it does not already exist,
/// and returns a boxed instance of `Wal` that can be used to manage WAL entries.
///
/// # Arguments
///
/// * `entry_path` - The path where the WAL directory should be created.
///
/// # Returns
///
/// A boxed instance of `Wal` that implements `Send` and `Sync`.
///
/// # Panics
///
/// This function will panic if it fails to create the WAL directory.
pub(in crate::storage) fn create_wal(entry_path: PathBuf) -> Box<dyn Wal + Send + Sync> {
    let wal_folder = entry_path.join("wal");
    if !wal_folder.exists() {
        std::fs::create_dir_all(&wal_folder).expect("Failed to create WAL folder");
    }
    Box::new(WalImpl::new(entry_path.join("wal")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use reduct_base::error::ErrorCode;
    use rstest::*;

    #[rstest]
    #[tokio::test]
    async fn test_read() {
        let path = tempfile::tempdir().unwrap();
        let mut wal = create_wal(path.path().to_path_buf());
        wal.append(1, WalEntry::WriteRecord(Record::default()))
            .await
            .unwrap();
        wal.append(1, WalEntry::UpdateRecord(Record::default()))
            .await
            .unwrap();
        wal.append(1, WalEntry::RemoveBlock).await.unwrap();

        let mut wal = create_wal(path.path().to_path_buf());
        let entries = wal.read(1).await.unwrap();

        assert_eq!(
            entries,
            vec![
                WalEntry::WriteRecord(Record::default()),
                WalEntry::UpdateRecord(Record::default()),
                WalEntry::RemoveBlock,
            ]
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove() {
        let path = tempfile::tempdir().unwrap();
        let mut wal = create_wal(path.path().to_path_buf());
        wal.append(1, WalEntry::WriteRecord(Record::default()))
            .await
            .unwrap();

        let mut wal = create_wal(path.path().to_path_buf());
        assert_eq!(wal.read(1).await.unwrap().len(), 1);
        wal.remove(1).await.unwrap();

        let mut wal = create_wal(path.path().to_path_buf());
        let err = wal.read(1).await.err().unwrap();
        assert_eq!(&err.status, &ErrorCode::InternalServerError);
    }
}
