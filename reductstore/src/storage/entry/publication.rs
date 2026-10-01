// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::core::file_cache::{BatchToken, FILE_CACHE};
use crate::storage::proto::{entry_publication, EntryPublication};
use prost::Message;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use reduct_base::too_early;
use std::io::{Read, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub(crate) const PUBLICATION_FILE: &str = ".publication";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PublicationState {
    Updating,
    Ready,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Publication {
    pub(super) incarnation: String,
    pub(super) generation: u64,
    pub(super) state: PublicationState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PublicationId {
    pub(crate) incarnation: String,
    pub(crate) generation: u64,
}

impl Publication {
    pub(super) fn new() -> Self {
        Self {
            incarnation: uuid::Uuid::new_v4().to_string(),
            generation: 0,
            state: PublicationState::Ready,
        }
    }

    pub(super) fn updating(&self) -> Result<Self, ReductError> {
        Ok(Self {
            incarnation: self.incarnation.clone(),
            generation: self
                .generation
                .checked_add(1)
                .ok_or_else(|| internal_server_error!("Entry publication generation overflow"))?,
            state: PublicationState::Updating,
        })
    }

    pub(super) fn ready(&self) -> Result<Self, ReductError> {
        Ok(Self {
            incarnation: self.incarnation.clone(),
            generation: self
                .generation
                .checked_add(1)
                .ok_or_else(|| internal_server_error!("Entry publication generation overflow"))?,
            state: PublicationState::Ready,
        })
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        EntryPublication {
            incarnation: self.incarnation.clone(),
            generation: self.generation,
            state: match self.state {
                PublicationState::Updating => entry_publication::State::Updating as i32,
                PublicationState::Ready => entry_publication::State::Ready as i32,
            },
        }
        .encode_to_vec()
    }

    pub(crate) fn id(&self) -> Option<PublicationId> {
        (self.state == PublicationState::Ready).then(|| PublicationId {
            incarnation: self.incarnation.clone(),
            generation: self.generation,
        })
    }

    fn decode(bytes: &[u8], path: &Path) -> Result<Self, ReductError> {
        let marker = EntryPublication::decode(bytes).map_err(|err| {
            internal_server_error!("Failed to decode entry publication {:?}: {}", path, err)
        })?;
        if marker.incarnation.is_empty() {
            return Err(internal_server_error!(
                "Entry publication {:?} has no incarnation",
                path
            ));
        }

        let state = match entry_publication::State::try_from(marker.state).ok() {
            Some(entry_publication::State::Updating) => PublicationState::Updating,
            Some(entry_publication::State::Ready) => PublicationState::Ready,
            None => {
                return Err(internal_server_error!(
                    "Entry publication {:?} has an invalid state",
                    path
                ));
            }
        };

        let valid_parity = match state {
            PublicationState::Updating => marker.generation % 2 == 1,
            PublicationState::Ready => marker.generation % 2 == 0,
        };
        if !valid_parity {
            return Err(internal_server_error!(
                "Entry publication {:?} has invalid generation parity",
                path
            ));
        }

        Ok(Self {
            incarnation: marker.incarnation,
            generation: marker.generation,
            state,
        })
    }
}

pub(crate) fn path(entry_path: &Path) -> PathBuf {
    entry_path.join(PUBLICATION_FILE)
}

/// Missing markers designate legacy entries and intentionally do not mutate them.
pub(super) async fn load(entry_path: &Path) -> Result<Option<Publication>, ReductError> {
    let path = path(entry_path);
    if !FILE_CACHE.try_exists(&path).await? {
        return Ok(None);
    }

    let mut file = FILE_CACHE.read(&path, SeekFrom::Start(0)).await?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Publication::decode(&bytes, &path).map(Some)
}

/// Loads a marker from the backend rather than a potentially stale local copy.
pub(crate) async fn load_fresh(entry_path: &Path) -> Result<Option<Publication>, ReductError> {
    let marker_path = path(entry_path);
    FILE_CACHE.invalidate_local_cache_file(&marker_path).await?;
    if !FILE_CACHE.try_exists(&marker_path).await? {
        return Ok(None);
    }

    let mut file = FILE_CACHE
        .read(&marker_path, SeekFrom::Start(0))
        .await
        .map_err(|err| {
            too_early!(
                "Entry publication {:?} disappeared while being read: {}",
                marker_path,
                err
            )
        })?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Publication::decode(&bytes, &marker_path).map(Some)
}

/// Validates that no publication occurred while a replica loaded an index.
pub(in crate::storage) fn validate_window(
    before: Option<Publication>,
    after: Option<Publication>,
) -> Result<crate::storage::block_manager::ReplicaPublication, ReductError> {
    match (before, after) {
        (None, None) => Ok(crate::storage::block_manager::ReplicaPublication::Legacy),
        (Some(before), Some(after))
            if before.state == PublicationState::Ready
                && after.state == PublicationState::Ready
                && before.id() == after.id() =>
        {
            Ok(
                crate::storage::block_manager::ReplicaPublication::Published(
                    before.id().expect("ready publication has an identity"),
                ),
            )
        }
        _ => Err(too_early!(
            "Entry publication changed while reloading replica index"
        )),
    }
}

pub(super) async fn write_local(
    entry_path: &Path,
    publication: &Publication,
) -> Result<(), ReductError> {
    let path = path(entry_path);
    let mut file = FILE_CACHE
        .write_or_create(&path, SeekFrom::Start(0))
        .await?;
    file.set_len(0)?;
    file.write_all(&publication.encode())?;
    file.flush_local().await?;
    Ok(())
}

pub(super) async fn write_local_in_batch(
    token: &BatchToken,
    entry_path: &Path,
    publication: &Publication,
) -> Result<PathBuf, ReductError> {
    let path = path(entry_path);
    let mut file = FILE_CACHE
        .write_or_create_in_batch(token, &path, SeekFrom::Start(0))
        .await?;
    file.set_len(0)?;
    file.write_all(&publication.encode())?;
    file.flush_local().await?;
    Ok(path)
}
