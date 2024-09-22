// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::PathBuf;

use async_trait::async_trait;
use crc64fast::Digest;
use log::warn;
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use reduct_base::error::ReductError;
use reduct_base::internal_server_error;

use crate::storage::file_cache::get_global_file_cache;
use crate::storage::proto::Record;

const WAL_FILE_SIZE: u64 = 1_000_000;

#[derive(PartialEq, Debug)]
pub(in crate::storage) enum WalEntry {
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
///
/// The WAL is used to store changes to blocks that have not been written to the block file yet.
///
/// Format in big-endian:
///
/// | Entry Type (8) | Entry Length (64) | Entry Data (variable) | CRC (64) | Stop Marker (8) |
///
#[async_trait]
pub(in crate::storage) trait Wal {
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

    /// List all WALs
    async fn list(&self) -> Result<Vec<u64>, ReductError>;

    /// Check if a WAL file exists for a block
    fn exists(&self, block_id: u64) -> bool;
}

struct WalImpl {
    root_path: PathBuf,
    file_positions: HashMap<u64, u64>,
}

impl WalImpl {
    pub fn new(path_buf: PathBuf) -> Self {
        WalImpl {
            root_path: path_buf,
            file_positions: HashMap::new(), // we need to keep track of the file positions for each block because of file cache
        }
    }

    fn block_wal_path(&self, block_id: u64) -> PathBuf {
        self.root_path.join(format!("{}.wal", block_id))
    }
}

const STOP_MARKER: u8 = 255;

#[async_trait]
impl Wal for WalImpl {
    async fn append(&mut self, block_id: u64, entry: WalEntry) -> Result<(), ReductError> {
        let path = self.block_wal_path(block_id);
        let file = if !path.exists() {
            let file = get_global_file_cache().write_or_create(&path).await?;
            // preallocate file to speed up writes
            file.write().await.set_len(WAL_FILE_SIZE).await?;
            self.file_positions.insert(block_id, 0);
            file
        } else {
            let file = get_global_file_cache().write_or_create(&path).await?;
            let pos = match self.file_positions.entry(block_id) {
                Occupied(e) => e.get().clone(),
                Vacant(e) => {
                    warn!(
                        "File position for block {} not found. Overwrite WAL",
                        block_id
                    );
                    e.insert(0).clone()
                }
            };

            file.write().await.seek(SeekFrom::Start(pos)).await?;
            file
        };

        let mut lock = file.write().await;
        if lock.stream_position().await? > 0 {
            // remove stop marker
            lock.seek(SeekFrom::Current(-1)).await?;
        }

        let buf = entry.encode();
        // write entry
        lock.write_all(&buf).await?;
        // write crc
        let mut crc = Digest::new();
        crc.write(&buf);
        lock.write(&crc.sum64().to_be_bytes()).await?;
        // write stop marker
        lock.write_u8(STOP_MARKER).await?;
        self.file_positions
            .insert(block_id, lock.stream_position().await?);
        Ok(())
    }

    async fn read(&self, block_id: u64) -> Result<Vec<WalEntry>, ReductError> {
        let path = self.block_wal_path(block_id);
        let file = get_global_file_cache().read(&path).await?;
        let mut lock = file.write().await;
        lock.seek(SeekFrom::Start(0)).await?;

        let mut entries = Vec::new();
        loop {
            // read entry type
            let entry_type = match lock.read_u8().await {
                Ok(t) => t,
                Err(err) => return Err(err.into()),
            };

            if entry_type == STOP_MARKER {
                break;
            }

            let mut crc = Digest::new();
            crc.write(&[entry_type]);

            // read entry length
            let len = lock.read_u64().await?;
            crc.write(&len.to_be_bytes());

            // read entry data
            let mut buf = vec![0; len as usize];
            lock.read_exact(&mut buf).await?;
            crc.write(&buf);

            // read crc
            let crc_bytes = lock.read_u64().await?;

            if crc.sum64() != crc_bytes {
                return Err(internal_server_error!("WAL {:?} is corrupted", path));
            }

            let entry = WalEntry::decode(entry_type, &buf)?;
            entries.push(entry);
        }

        Ok(entries)
    }

    async fn remove(&self, block_id: u64) -> Result<(), ReductError> {
        let path = self.block_wal_path(block_id);
        get_global_file_cache().remove(&path).await?;
        Ok(())
    }

    async fn list(&self) -> Result<Vec<u64>, ReductError> {
        let mut blocks = Vec::new();
        for entry in std::fs::read_dir(&self.root_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().unwrap_or_default() == "wal" {
                let block_id = path
                    .file_stem()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap();
                blocks.push(block_id);
            }
        }
        Ok(blocks)
    }

    fn exists(&self, block_id: u64) -> bool {
        let path = self.block_wal_path(block_id);
        path.try_exists().unwrap_or(false)
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
    if !wal_folder.try_exists().unwrap() {
        std::fs::create_dir_all(&wal_folder).expect("Failed to create WAL folder");
    }
    Box::new(WalImpl::new(entry_path.join("wal")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use reduct_base::error::ErrorCode;
    use rstest::*;
    use tokio::fs;

    #[rstest]
    #[tokio::test]
    async fn test_read(mut wal: WalImpl) {
        wal.append(1, WalEntry::WriteRecord(Record::default()))
            .await
            .unwrap();
        wal.append(1, WalEntry::UpdateRecord(Record::default()))
            .await
            .unwrap();
        wal.append(1, WalEntry::RemoveBlock).await.unwrap();

        let wal = create_wal(wal.root_path.parent().unwrap().to_path_buf());
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
    async fn test_remove(mut wal: WalImpl) {
        wal.append(1, WalEntry::WriteRecord(Record::default()))
            .await
            .unwrap();

        assert_eq!(wal.read(1).await.unwrap().len(), 1);
        wal.remove(1).await.unwrap();

        let wal = create_wal(wal.root_path.parent().unwrap().to_path_buf());
        let err = wal.read(1).await.err().unwrap();
        assert_eq!(&err.status, &ErrorCode::InternalServerError);
    }

    #[rstest]
    #[tokio::test]
    async fn test_list(mut wal: WalImpl) {
        wal.append(1, WalEntry::WriteRecord(Record::default()))
            .await
            .unwrap();
        wal.append(2, WalEntry::WriteRecord(Record::default()))
            .await
            .unwrap();

        let wal = create_wal(wal.root_path.parent().unwrap().to_path_buf());
        let blocks = wal.list().await.unwrap();
        assert_eq!(blocks.len(), 2);
        assert!(blocks.contains(&1));
        assert!(blocks.contains(&2));
    }

    #[rstest]
    #[tokio::test]
    async fn test_exists(mut wal: WalImpl) {
        wal.append(1, WalEntry::WriteRecord(Record::default()))
            .await
            .unwrap();

        assert!(wal.exists(1));
        assert!(!wal.exists(2));
    }

    #[rstest]
    #[tokio::test]
    async fn test_crc_error(mut wal: WalImpl) {
        wal.append(1, WalEntry::WriteRecord(Record::default()))
            .await
            .unwrap();

        let path = wal.block_wal_path(1);
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .await
            .unwrap();
        file.seek(SeekFrom::Start(0)).await.unwrap();
        file.write_all(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
            .await
            .unwrap();

        let wal = create_wal(wal.root_path.parent().unwrap().to_path_buf());
        let err = wal.read(1).await.err().unwrap();
        assert_eq!(&err.status, &ErrorCode::InternalServerError);
    }

    #[fixture]
    fn wal() -> WalImpl {
        let path = tempfile::tempdir().unwrap().into_path();
        std::fs::create_dir_all(path.join("wal")).unwrap();
        WalImpl::new(path.join("wal"))
    }
}
