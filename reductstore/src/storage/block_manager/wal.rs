// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::PathBuf;

use async_trait::async_trait;
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use reduct_base::error::ReductError;
use reduct_base::internal_server_error;

use crate::storage::file_cache::get_global_file_cache;
use crate::storage::proto::Record;

const WAL_FILE_SIZE: u64 = 1_000_000;
pub(super) enum WalEntry {
    WriteRecord(Record),
    UpdateRecord(Record),
    RemoveBlock,
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

    /// Clean the WAL file for a block
    async fn clean(&self, block_id: u64) -> Result<(), ReductError>;
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
        let file = if !path.exists() {
            let file = get_global_file_cache().write_or_create(&path).await?;
            // preallocate file to speed up writes
            file.write().await.set_len(WAL_FILE_SIZE).await?;
            file
        } else {
            get_global_file_cache().read(&path).await?
        };

        let mut lock = file.write().await;
        match entry {
            WalEntry::WriteRecord(record) => {
                lock.write_u8(0).await?;
                let buf = record.encode_to_vec();
                lock.write_u64(buf.len() as u64).await?;
                lock.write_all(&buf).await?;
            }
            WalEntry::UpdateRecord(record) => {
                lock.write_u8(1).await?;
                let buf = record.encode_to_vec();
                lock.write_u64(buf.len() as u64).await?;
                lock.write_all(&buf).await?;
            }
            WalEntry::RemoveBlock => {
                lock.write_u8(2).await?;
            }
        }
        Ok(())
    }

    async fn read(&self, block_id: u64) -> Result<Vec<WalEntry>, ReductError> {
        let path = self.block_wal_path(block_id);
        let file = get_global_file_cache().read(&path).await?;
        let mut lock = file.write().await;
        lock.seek(SeekFrom::Start(0)).await?;

        let mut entries = Vec::new();
        while lock.seek(SeekFrom::Current(0)).await? < lock.seek(SeekFrom::End(0)).await? {
            let mut buf = [0u8; 1];
            lock.read_exact(&mut buf).await?;

            let entry = match buf[0] {
                0 => {
                    let mut len_buf = [0u8; 8];
                    lock.read_exact(&mut len_buf).await?;
                    let len = u64::from_le_bytes(len_buf);
                    let mut buf = vec![0u8; len as usize];
                    lock.read_exact(&mut buf).await?;
                    WalEntry::WriteRecord(Record::decode(&buf[..]).unwrap())
                }
                1 => {
                    let mut len_buf = [0u8; 8];
                    lock.read_exact(&mut len_buf).await?;
                    let len = u64::from_le_bytes(len_buf);
                    let mut buf = vec![0u8; len as usize];
                    lock.read_exact(&mut buf).await?;
                    WalEntry::UpdateRecord(Record::decode(&buf[..]).unwrap())
                }
                2 => WalEntry::RemoveBlock,
                _ => return Err(internal_server_error!("Invalid WAL entry")),
            };
            entries.push(entry);
        }

        Ok(entries)
    }

    async fn clean(&self, block_id: u64) -> Result<(), ReductError> {
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
