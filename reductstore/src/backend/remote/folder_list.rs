// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::backend::remote::proto::FolderListing;
use log::info;
use prost::Message;
use std::path::PathBuf;

pub(super) fn remove_folder_from_list(
    folder_list_path: &PathBuf,
    folder_name: &str,
) -> std::io::Result<()> {
    let listing = std::fs::read(&folder_list_path)?;

    let mut crc64 = crc64fast::Digest::new();
    let mut list = FolderListing::decode(listing.as_slice())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    for entry in &list.entries {
        crc64.write(entry.as_bytes());
    }

    if list.crc64 != crc64.sum64() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Folder listing CRC64 mismatch",
        ));
    }

    let filtered_folders: Vec<String> = list
        .entries
        .into_iter()
        .filter(|name| name != folder_name)
        .collect();

    list.entries = filtered_folders;
    list.crc64 = {
        let mut new_crc64 = crc64fast::Digest::new();
        for entry in &list.entries {
            new_crc64.write(entry.as_bytes());
        }
        new_crc64.sum64()
    };
    let mut buf = Vec::new();
    list.encode(&mut buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    std::fs::write(folder_list_path, buf)?;

    info!("Removed folder '{}' from listing", folder_name);
    Ok(())
}

pub(super) fn add_folder_to_list(
    folder_list_path: &PathBuf,
    folder_name: &str,
) -> std::io::Result<()> {
    let listing = std::fs::read(&folder_list_path).unwrap_or(Vec::new());

    let mut crc64 = crc64fast::Digest::new();
    let mut list = if listing.is_empty() {
        FolderListing {
            entries: vec![],
            crc64: 0,
        }
    } else {
        FolderListing::decode(listing.as_slice())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
    };

    for entry in &list.entries {
        crc64.write(entry.as_bytes());
    }

    if list.crc64 != crc64.sum64() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Folder listing CRC64 mismatch",
        ));
    }

    if !list.entries.contains(&folder_name.to_string()) {
        list.entries.push(folder_name.to_string());
    }

    list.crc64 = {
        let mut new_crc64 = crc64fast::Digest::new();
        for entry in &list.entries {
            new_crc64.write(entry.as_bytes());
        }
        new_crc64.sum64()
    };
    let mut buf = Vec::new();
    list.encode(&mut buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    std::fs::write(folder_list_path, buf)?;

    info!("Added folder '{}' to listing", folder_name);
    Ok(())
}

pub(super) fn list_folders(folder_list_path: &PathBuf) -> std::io::Result<Vec<String>> {
    let listing = std::fs::read(&folder_list_path)?;

    let mut crc64 = crc64fast::Digest::new();
    let list = if listing.is_empty() {
        FolderListing {
            entries: vec![],
            crc64: 0,
        }
    } else {
        FolderListing::decode(listing.as_slice())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
    };

    for entry in &list.entries {
        crc64.write(entry.as_bytes());
    }

    if list.crc64 != crc64.sum64() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Folder listing CRC64 mismatch",
        ));
    }

    info!("Listed folders: {:?}", list.entries);
    Ok(list.entries)
}
