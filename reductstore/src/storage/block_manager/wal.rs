// Copyright 2024-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use async_trait::async_trait;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem;
use std::path::PathBuf;

use crc64fast::Digest;
use log::warn;
use prost::Message;

use reduct_base::error::ReductError;
use reduct_base::internal_server_error;

use crate::core::file_cache::FILE_CACHE;
use crate::storage::proto::Record;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

const WAL_FILE_SIZE: u64 = 1_000_000;

#[derive(PartialEq, Debug)]
pub(in crate::storage) enum WalEntry {
    WriteRecord(Record),
    UpdateRecord(Record),
    RemoveBlock,
    RemoveRecord(u64),
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
            WalEntry::RemoveRecord(record_id) => {
                let mut buf = vec![3];
                buf.extend_from_slice(&(mem::size_of_val(record_id) as u64).to_be_bytes());
                buf.extend_from_slice(&record_id.to_be_bytes());
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
            3 => {
                let record_id = u64::from_be_bytes(buf.try_into().unwrap());
                Ok(WalEntry::RemoveRecord(record_id))
            }
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
        let mut file = if !FILE_CACHE.try_exists(&path).await? {
            let mut file = FILE_CACHE
                .write_or_create(&path, SeekFrom::Current(0))
                .await?;
            file.set_len(WAL_FILE_SIZE)?;
            self.file_positions.insert(block_id, 0);
            file
        } else {
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

            FILE_CACHE
                .write_or_create(&path, SeekFrom::Start(pos))
                .await?
        };

        if file.stream_position()? > 0 {
            file.seek(SeekFrom::Current(-1))?;
        }

        let buf = entry.encode();
        file.write_all(&buf)?;
        let mut crc = Digest::new();
        crc.write(&buf);
        file.write(&crc.sum64().to_be_bytes())?;
        file.write_u8(STOP_MARKER)?;
        self.file_positions
            .insert(block_id, file.stream_position()?);
        Ok(())
    }

    async fn read(&self, block_id: u64) -> Result<Vec<WalEntry>, ReductError> {
        let path = self.block_wal_path(block_id);
        let mut file = FILE_CACHE.read(&path, SeekFrom::Start(0)).await?;

        let mut entries = Vec::new();
        loop {
            let entry_type = match file.read_u8() {
                Ok(t) => t,
                Err(err) => return Err(err.into()),
            };

            if entry_type == STOP_MARKER {
                break;
            }

            let mut crc = Digest::new();
            crc.write(&[entry_type]);

            let len = file.read_u64::<BigEndian>()?;
            crc.write(&len.to_be_bytes());

            let mut buf = vec![0; len as usize];
            file.read_exact(&mut buf)?;
            crc.write(&buf);

            let crc_bytes = file.read_u64::<BigEndian>()?;

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
        if FILE_CACHE.try_exists(&path).await? {
            FILE_CACHE.remove(&path).await?;
        }
        Ok(())
    }

    async fn list(&self) -> Result<Vec<u64>, ReductError> {
        let mut blocks = Vec::new();
        for path in FILE_CACHE.read_dir(&self.root_path).await? {
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
pub(in crate::storage) async fn create_wal(entry_path: PathBuf) -> Box<dyn Wal + Send + Sync> {
    let wal_folder = entry_path.join("wal");
    if !wal_folder.try_exists().unwrap() {
        FILE_CACHE
            .create_dir_all(&wal_folder)
            .await
            .expect("Failed to create WAL folder");
    }
    Box::new(WalImpl::new(entry_path.join("wal")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use reduct_base::error::ErrorCode;
    use rstest::*;
    use std::fs::OpenOptions;

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
        wal.append(1, WalEntry::RemoveRecord(1)).await.unwrap();

        let wal = create_wal(wal.root_path.parent().unwrap().to_path_buf());
        let entries = wal.await.read(1).await.unwrap();

        assert_eq!(
            entries,
            vec![
                WalEntry::WriteRecord(Record::default()),
                WalEntry::UpdateRecord(Record::default()),
                WalEntry::RemoveBlock,
                WalEntry::RemoveRecord(1)
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
        let err = wal.await.read(1).await.err().unwrap();
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
        let blocks = wal.await.list().await.unwrap();
        assert_eq!(blocks.len(), 2);
        assert!(blocks.contains(&1));
        assert!(blocks.contains(&2));
    }

    #[rstest]
    #[tokio::test]
    async fn test_crc_error(mut wal: WalImpl) {
        wal.append(1, WalEntry::WriteRecord(Record::default()))
            .await
            .unwrap();

        let path = wal.block_wal_path(1);
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 1]).unwrap();

        let wal = create_wal(wal.root_path.parent().unwrap().to_path_buf());
        let err = wal.await.read(1).await.err().unwrap();
        assert_eq!(&err.status, &ErrorCode::InternalServerError);
    }

    #[rstest]
    #[tokio::test]
    async fn cache_invalidation(mut wal: WalImpl) {
        wal.append(1, WalEntry::UpdateRecord(Record::default()))
            .await
            .unwrap();
        FILE_CACHE
            .discard_recursive(&wal.root_path.join("1.wal"))
            .await
            .unwrap();
        wal.append(1, WalEntry::WriteRecord(Record::default()))
            .await
            .unwrap();

        let entries = wal.read(1).await.unwrap();
        assert_eq!(
            entries,
            vec![
                WalEntry::UpdateRecord(Record::default()),
                WalEntry::WriteRecord(Record::default())
            ],
            "We keep entry after cache invalidation"
        );
    }

    #[fixture]
    fn wal() -> WalImpl {
        let path = tempfile::tempdir().unwrap().keep();
        std::fs::create_dir_all(path.join("wal")).unwrap();
        WalImpl::new(path.join("wal"))
    }
}
