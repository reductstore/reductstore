// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use bytes::Bytes;
use crc64fast::Digest;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::storage::proto::{
    ts_to_us, Block as BlockProto, BlockIndex as BlockIndexProto, BucketSettings, MinimalBlock,
};

const BUCKET_SETTINGS_FILE: &str = "bucket.settings";
const BLOCK_INDEX_FILE: &str = "blocks.idx";
const DESCRIPTOR_FILE_EXT: &str = ".meta";
const DATA_FILE_EXT: &str = ".blk";
const WAL_DIR: &str = ".wal";
const LEGACY_WAL_DIR: &str = "wal";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DoctorReport {
    pub data_path: PathBuf,
    pub summary: DoctorSummary,
    pub buckets: Vec<DoctorBucketReport>,
    pub issues: Vec<DoctorIssue>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DoctorSummary {
    pub bucket_count: usize,
    pub entry_count: usize,
    pub block_count: usize,
    pub error_count: usize,
    pub warning_count: usize,
    pub info_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DoctorBucketReport {
    pub name: String,
    pub path: PathBuf,
    pub entries: Vec<DoctorEntryReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DoctorEntryReport {
    pub bucket: String,
    pub name: String,
    pub path: PathBuf,
    pub block_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DoctorIssue {
    pub severity: DoctorSeverity,
    pub kind: DoctorIssueKind,
    pub bucket: Option<String>,
    pub entry: Option<String>,
    pub block_id: Option<u64>,
    pub path: PathBuf,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DoctorSeverity {
    Error,
    Warning,
    Info,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DoctorIssueKind {
    MissingDataPath,
    DatabaseMayBeLive,
    UnreadableBucketSettings,
    InvalidBucketSettings,
    MissingBlockIndex,
    CorruptBlockIndex,
    IndexReferencesMissingDescriptor,
    IndexReferencesMissingDataBlock,
    DescriptorDecodeFailed,
    DescriptorCrcMismatch,
    DescriptorBlockIdMismatch,
    DescriptorNotIndexed,
    DataBlockNotIndexed,
    DataBlockTooSmall,
    DirtyWalPresent,
    WalDecodeFailed,
    WalForUnknownBlock,
    LegacyWalDirectoryPresent,
    OrphanTempFile,
    UnknownEntryInternalFile,
}

/// Scan a local filesystem ReductStore data path without mutating it.
///
/// This implementation performs:
/// - bucket discovery and settings presence checks,
/// - recursive entry discovery by `blocks.idx`,
/// - Phase B: block index CRC verification,
/// - Phase C: descriptor/data relationship and CRC checks,
/// - orphan descriptor/data file detection.
pub fn scan_data_path(data_path: impl AsRef<Path>) -> io::Result<DoctorReport> {
    let data_path = data_path.as_ref().to_path_buf();
    let mut report = DoctorReport {
        data_path: data_path.clone(),
        summary: DoctorSummary::default(),
        buckets: Vec::new(),
        issues: Vec::new(),
    };

    if !data_path.exists() {
        report.push_issue(DoctorIssue {
            severity: DoctorSeverity::Error,
            kind: DoctorIssueKind::MissingDataPath,
            bucket: None,
            entry: None,
            block_id: None,
            path: data_path,
            message: "Data path does not exist".to_string(),
        });
        return Ok(report);
    }

    for bucket_entry in fs::read_dir(&data_path)? {
        let bucket_entry = bucket_entry?;
        let bucket_path = bucket_entry.path();
        if !bucket_path.is_dir() || is_hidden_path(&bucket_path) {
            continue;
        }

        let Some(bucket_name) = path_file_name(&bucket_path) else {
            continue;
        };

        check_bucket_settings(&bucket_name, &bucket_path, &mut report);

        let entries = discover_entries(&bucket_name, &bucket_path)?;
        for entry in &entries {
            check_entry(&mut report, entry);
        }
        report.summary.entry_count += entries.len();
        report.buckets.push(DoctorBucketReport {
            name: bucket_name,
            path: bucket_path,
            entries,
        });
    }

    report.summary.bucket_count = report.buckets.len();
    Ok(report)
}

impl DoctorReport {
    fn push_issue(&mut self, issue: DoctorIssue) {
        match issue.severity {
            DoctorSeverity::Error => self.summary.error_count += 1,
            DoctorSeverity::Warning => self.summary.warning_count += 1,
            DoctorSeverity::Info => self.summary.info_count += 1,
        }
        self.issues.push(issue);
    }
}

fn discover_entries(bucket_name: &str, bucket_path: &Path) -> io::Result<Vec<DoctorEntryReport>> {
    let mut entries = Vec::new();
    discover_entries_recursive(bucket_name, bucket_path, bucket_path, &mut entries)?;
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(entries)
}

fn discover_entries_recursive(
    bucket_name: &str,
    bucket_path: &Path,
    current_path: &Path,
    entries: &mut Vec<DoctorEntryReport>,
) -> io::Result<()> {
    if current_path != bucket_path && current_path.join(BLOCK_INDEX_FILE).is_file() {
        if let Ok(relative) = current_path.strip_prefix(bucket_path) {
            entries.push(DoctorEntryReport {
                bucket: bucket_name.to_string(),
                name: normalize_entry_name(relative),
                path: current_path.to_path_buf(),
                block_count: 0,
            });
        }
    }

    for child in fs::read_dir(current_path)? {
        let child = child?;
        let child_path = child.path();
        if !child_path.is_dir() || is_internal_entry_dir(&child_path) {
            continue;
        }

        discover_entries_recursive(bucket_name, bucket_path, &child_path, entries)?;
    }

    Ok(())
}

fn check_entry(report: &mut DoctorReport, entry: &DoctorEntryReport) {
    let bucket_name = Some(entry.bucket.clone());
    let entry_name = Some(entry.name.clone());

    let index_path = entry.path.join(BLOCK_INDEX_FILE);
    let Some(index) = read_and_verify_block_index(&index_path, report, bucket_name, entry_name)
    else {
        return;
    };

    let mut block_count = 0;
    for (block_id, block_info) in &index {
        block_count += 1;
        let descriptor_path = entry.path.join(format!("{block_id}{DESCRIPTOR_FILE_EXT}"));
        let data_path = entry.path.join(format!("{block_id}{DATA_FILE_EXT}"));

        if !descriptor_path.is_file() {
            report.push_issue(DoctorIssue {
                severity: DoctorSeverity::Error,
                kind: DoctorIssueKind::IndexReferencesMissingDescriptor,
                bucket: Some(entry.bucket.clone()),
                entry: Some(entry.name.clone()),
                block_id: Some(*block_id),
                path: descriptor_path.clone(),
                message: format!(
                    "Block index references block {block_id} but descriptor is missing"
                ),
            });
        }
        if !data_path.is_file() {
            report.push_issue(DoctorIssue {
                severity: DoctorSeverity::Error,
                kind: DoctorIssueKind::IndexReferencesMissingDataBlock,
                bucket: Some(entry.bucket.clone()),
                entry: Some(entry.name.clone()),
                block_id: Some(*block_id),
                path: data_path,
                message: format!(
                    "Block index references block {block_id} but data block is missing"
                ),
            });
        }

        check_descriptor(
            report,
            entry,
            *block_id,
            &descriptor_path,
            block_info.descriptor_crc.as_ref(),
        );
    }
    report.summary.block_count += block_count;

    let Ok(children) = fs::read_dir(&entry.path) else {
        return;
    };
    for child in children.flatten() {
        let path = child.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name == BLOCK_INDEX_FILE {
            continue;
        }
        if name.starts_with('.') {
            continue;
        }
        if let Some(block_id) = parse_block_id(name, DESCRIPTOR_FILE_EXT) {
            if !index.contains_key(&block_id) {
                report.push_issue(DoctorIssue {
                    severity: DoctorSeverity::Warning,
                    kind: DoctorIssueKind::DescriptorNotIndexed,
                    bucket: Some(entry.bucket.clone()),
                    entry: Some(entry.name.clone()),
                    block_id: Some(block_id),
                    path: path.clone(),
                    message: format!("Descriptor `{name}` is not referenced by the block index"),
                });
            }
        } else if let Some(block_id) = parse_block_id(name, DATA_FILE_EXT) {
            if !index.contains_key(&block_id) {
                report.push_issue(DoctorIssue {
                    severity: DoctorSeverity::Warning,
                    kind: DoctorIssueKind::DataBlockNotIndexed,
                    bucket: Some(entry.bucket.clone()),
                    entry: Some(entry.name.clone()),
                    block_id: Some(block_id),
                    path: path.clone(),
                    message: format!("Data block `{name}` is not referenced by the block index"),
                });
            }
        } else {
            report.push_issue(DoctorIssue {
                severity: DoctorSeverity::Info,
                kind: DoctorIssueKind::UnknownEntryInternalFile,
                bucket: Some(entry.bucket.clone()),
                entry: Some(entry.name.clone()),
                block_id: None,
                path: path.clone(),
                message: format!("Unrecognized file `{name}` in entry directory"),
            });
        }
    }
}

fn read_and_verify_block_index(
    path: &Path,
    report: &mut DoctorReport,
    bucket: Option<String>,
    entry: Option<String>,
) -> Option<BTreeMap<u64, BlockIndexEntry>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => Bytes::from(bytes),
        Err(err) => {
            report.push_issue(DoctorIssue {
                severity: DoctorSeverity::Error,
                kind: DoctorIssueKind::MissingBlockIndex,
                bucket,
                entry,
                block_id: None,
                path: path.to_path_buf(),
                message: format!("Failed to read {}: {}", BLOCK_INDEX_FILE, err),
            });
            return None;
        }
    };

    if bytes.is_empty() {
        // The project treats an empty block index as corruption when descriptors
        // exist alongside it. Match that behavior so the doctor agrees with
        // startup recovery.
        if let Some(parent) = path.parent() {
            if let Ok(entries) = fs::read_dir(parent) {
                let has_descriptors = entries.flatten().any(|entry| {
                    entry
                        .path()
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.ends_with(DESCRIPTOR_FILE_EXT))
                });
                if has_descriptors {
                    report.push_issue(DoctorIssue {
                        severity: DoctorSeverity::Error,
                        kind: DoctorIssueKind::CorruptBlockIndex,
                        bucket,
                        entry,
                        block_id: None,
                        path: path.to_path_buf(),
                        message: format!(
                            "{} is empty but descriptor files exist alongside it",
                            BLOCK_INDEX_FILE
                        ),
                    });
                    return None;
                }
            }
        }
        return Some(BTreeMap::new());
    }

    let proto = match BlockIndexProto::decode(bytes) {
        Ok(proto) => proto,
        Err(err) => {
            report.push_issue(DoctorIssue {
                severity: DoctorSeverity::Error,
                kind: DoctorIssueKind::CorruptBlockIndex,
                bucket,
                entry,
                block_id: None,
                path: path.to_path_buf(),
                message: format!("Failed to decode {}: {}", BLOCK_INDEX_FILE, err),
            });
            return None;
        }
    };

    let mut index: BTreeMap<u64, BlockIndexEntry> = BTreeMap::new();
    for block in proto.blocks {
        let latest = block
            .latest_record_time
            .as_ref()
            .map(ts_to_us)
            .unwrap_or(block.block_id);
        index.insert(
            block.block_id,
            BlockIndexEntry {
                size: block.size,
                record_count: block.record_count,
                metadata_size: block.metadata_size,
                descriptor_crc: block.crc64,
                latest_record_time_us: latest,
            },
        );
    }

    let computed_crc = compute_block_index_crc(&index);
    if computed_crc != proto.crc64 {
        report.push_issue(DoctorIssue {
            severity: DoctorSeverity::Error,
            kind: DoctorIssueKind::CorruptBlockIndex,
            bucket,
            entry,
            block_id: None,
            path: path.to_path_buf(),
            message: format!(
                "Block index CRC mismatch: expected {}, computed {}",
                proto.crc64, computed_crc
            ),
        });
    }

    Some(index)
}

struct BlockIndexEntry {
    size: u64,
    record_count: u64,
    metadata_size: u64,
    descriptor_crc: Option<u64>,
    latest_record_time_us: u64,
}

fn compute_block_index_crc(index: &BTreeMap<u64, BlockIndexEntry>) -> u64 {
    let mut crc = Digest::new();
    for (block_id, block) in index {
        crc.write(&block_id.to_be_bytes());
        crc.write(&block.size.to_be_bytes());
        crc.write(&block.record_count.to_be_bytes());
        crc.write(&block.metadata_size.to_be_bytes());
        crc.write(&block.latest_record_time_us.to_be_bytes());
        if let Some(c) = &block.descriptor_crc {
            crc.write(&c.to_be_bytes());
        }
    }
    crc.sum64()
}

fn check_descriptor(
    report: &mut DoctorReport,
    entry: &DoctorEntryReport,
    expected_block_id: u64,
    descriptor_path: &Path,
    expected_descriptor_crc: Option<&u64>,
) {
    let Ok(bytes) = fs::read(descriptor_path) else {
        return;
    };

    let mut crc = Digest::new();
    crc.write(&bytes);
    let computed = crc.sum64();
    if let Some(expected) = expected_descriptor_crc {
        if *expected != computed {
            report.push_issue(DoctorIssue {
                severity: DoctorSeverity::Error,
                kind: DoctorIssueKind::DescriptorCrcMismatch,
                bucket: Some(entry.bucket.clone()),
                entry: Some(entry.name.clone()),
                block_id: Some(expected_block_id),
                path: descriptor_path.to_path_buf(),
                message: format!(
                    "Descriptor CRC mismatch: expected {}, computed {}",
                    expected, computed
                ),
            });
        }
    }

    let proto: Option<DecodedDescriptor> = if !bytes.is_empty() {
        if let Ok(minimal) = MinimalBlock::decode(Bytes::from(bytes.clone())) {
            Some(DecodedDescriptor::Minimal(minimal))
        } else {
            BlockProto::decode(Bytes::from(bytes.clone()))
                .ok()
                .map(DecodedDescriptor::Full)
        }
    } else {
        None
    };

    let Some(proto) = proto else {
        report.push_issue(DoctorIssue {
            severity: DoctorSeverity::Error,
            kind: DoctorIssueKind::DescriptorDecodeFailed,
            bucket: Some(entry.bucket.clone()),
            entry: Some(entry.name.clone()),
            block_id: Some(expected_block_id),
            path: descriptor_path.to_path_buf(),
            message: "Failed to decode descriptor as MinimalBlock or Block".to_string(),
        });
        return;
    };

    let begin_time = match &proto {
        DecodedDescriptor::Minimal(m) => m.begin_time.clone(),
        DecodedDescriptor::Full(b) => b.begin_time.clone(),
    };

    if let Some(begin_time) = &begin_time {
        if ts_to_us(begin_time) != expected_block_id {
            report.push_issue(DoctorIssue {
                severity: DoctorSeverity::Error,
                kind: DoctorIssueKind::DescriptorBlockIdMismatch,
                bucket: Some(entry.bucket.clone()),
                entry: Some(entry.name.clone()),
                block_id: Some(expected_block_id),
                path: descriptor_path.to_path_buf(),
                message: format!(
                    "Descriptor begin_time {} does not match block id {}",
                    ts_to_us(begin_time),
                    expected_block_id
                ),
            });
        }
    } else {
        report.push_issue(DoctorIssue {
            severity: DoctorSeverity::Error,
            kind: DoctorIssueKind::DescriptorDecodeFailed,
            bucket: Some(entry.bucket.clone()),
            entry: Some(entry.name.clone()),
            block_id: Some(expected_block_id),
            path: descriptor_path.to_path_buf(),
            message: "Descriptor is missing begin_time".to_string(),
        });
    }

    let descriptor_size = match &proto {
        DecodedDescriptor::Minimal(m) => m.size,
        DecodedDescriptor::Full(b) => b.size,
    };

    let data_path = entry
        .path
        .join(format!("{expected_block_id}{DATA_FILE_EXT}"));
    if data_path.is_file() {
        if let Ok(data_meta) = fs::metadata(&data_path) {
            if data_meta.len() < descriptor_size {
                report.push_issue(DoctorIssue {
                    severity: DoctorSeverity::Error,
                    kind: DoctorIssueKind::DataBlockTooSmall,
                    bucket: Some(entry.bucket.clone()),
                    entry: Some(entry.name.clone()),
                    block_id: Some(expected_block_id),
                    path: data_path.clone(),
                    message: format!(
                        "Data block size {} is smaller than descriptor size {}",
                        data_meta.len(),
                        descriptor_size
                    ),
                });
            }
        }
    }
}

enum DecodedDescriptor {
    Minimal(MinimalBlock),
    Full(BlockProto),
}

fn parse_block_id(file_name: &str, ext: &str) -> Option<u64> {
    file_name
        .strip_suffix(ext)
        .and_then(|stem| stem.parse::<u64>().ok())
}

fn is_hidden_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with('.'))
}

fn is_internal_entry_dir(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with('.') || name == LEGACY_WAL_DIR || name == WAL_DIR)
}

fn normalize_entry_name(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn path_file_name(path: &Path) -> Option<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(ToString::to_string)
}

fn check_bucket_settings(bucket_name: &str, bucket_path: &Path, report: &mut DoctorReport) {
    let path = bucket_path.join(BUCKET_SETTINGS_FILE);
    let Ok(bytes) = fs::read(&path) else {
        report.push_issue(DoctorIssue {
            severity: DoctorSeverity::Error,
            kind: DoctorIssueKind::UnreadableBucketSettings,
            bucket: Some(bucket_name.to_string()),
            entry: None,
            block_id: None,
            path,
            message: format!("Bucket `{bucket_name}` has no readable {BUCKET_SETTINGS_FILE}"),
        });
        return;
    };

    if BucketSettings::decode(Bytes::from(bytes)).is_err() {
        report.push_issue(DoctorIssue {
            severity: DoctorSeverity::Error,
            kind: DoctorIssueKind::InvalidBucketSettings,
            bucket: Some(bucket_name.to_string()),
            entry: None,
            block_id: None,
            path,
            message: format!("Bucket `{bucket_name}` has corrupt {BUCKET_SETTINGS_FILE}"),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use tempfile::tempdir;

    fn build_index_bytes(entries: &[(u64, u64, u64, u64, Option<u64>)]) -> Vec<u8> {
        let mut proto = BlockIndexProto {
            blocks: Vec::new(),
            crc64: 0,
        };
        for (block_id, size, record_count, metadata_size, crc64) in entries {
            proto
                .blocks
                .push(crate::storage::proto::block_index::Block {
                    block_id: *block_id,
                    size: *size,
                    record_count: *record_count,
                    metadata_size: *metadata_size,
                    latest_record_time: Some(crate::storage::proto::us_to_ts(block_id)),
                    crc64: *crc64,
                });
        }
        let mut crc = Digest::new();
        for (block_id, size, record_count, metadata_size, crc64) in entries {
            crc.write(&block_id.to_be_bytes());
            crc.write(&size.to_be_bytes());
            crc.write(&record_count.to_be_bytes());
            crc.write(&metadata_size.to_be_bytes());
            crc.write(&block_id.to_be_bytes());
            if let Some(c) = crc64 {
                crc.write(&c.to_be_bytes());
            }
        }
        proto.crc64 = crc.sum64();
        proto.encode_to_vec()
    }

    fn build_minimal_block(begin_time: u64, size: u64) -> Vec<u8> {
        let proto = MinimalBlock {
            begin_time: Some(crate::storage::proto::us_to_ts(&begin_time)),
            latest_record_time: Some(crate::storage::proto::us_to_ts(&begin_time)),
            size,
            record_count: 0,
            metadata_size: 0,
        };
        proto.encode_to_vec()
    }

    #[test]
    fn doctor_missing_data_path() {
        let dir = tempdir().unwrap();
        let missing_path = dir.path().join("missing");

        let report = scan_data_path(&missing_path).unwrap();

        assert_eq!(report.summary.error_count, 1);
        assert_eq!(report.issues.len(), 1);
        assert_eq!(report.issues[0].kind, DoctorIssueKind::MissingDataPath);
    }

    #[test]
    fn doctor_empty_data_path() {
        let dir = tempdir().unwrap();

        let report = scan_data_path(dir.path()).unwrap();

        assert_eq!(report.summary.bucket_count, 0);
        assert_eq!(report.summary.entry_count, 0);
        assert!(report.issues.is_empty());
    }

    #[test]
    fn doctor_finds_nested_entries() {
        let dir = tempdir().unwrap();
        let bucket = dir.path().join("bucket-1");
        let entry_a = bucket.join("entry");
        let entry_b = bucket.join("entry").join("nested");
        fs::create_dir_all(&entry_b).unwrap();
        fs::write(bucket.join(BUCKET_SETTINGS_FILE), []).unwrap();
        fs::write(entry_a.join(BLOCK_INDEX_FILE), []).unwrap();
        fs::write(entry_b.join(BLOCK_INDEX_FILE), []).unwrap();

        let report = scan_data_path(dir.path()).unwrap();

        assert_eq!(report.summary.bucket_count, 1);
        assert_eq!(report.summary.entry_count, 2);
        assert!(report.issues.is_empty());
        assert_eq!(
            report.buckets[0]
                .entries
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["entry", "entry/nested"]
        );
    }

    #[test]
    fn doctor_skips_internal_wal_dir() {
        let dir = tempdir().unwrap();
        let bucket = dir.path().join("bucket-1");
        let entry = bucket.join("entry");
        let wal_dir = entry.join(WAL_DIR);
        fs::create_dir_all(&wal_dir).unwrap();
        fs::write(bucket.join(BUCKET_SETTINGS_FILE), []).unwrap();
        fs::write(entry.join(BLOCK_INDEX_FILE), []).unwrap();
        fs::write(wal_dir.join(BLOCK_INDEX_FILE), []).unwrap();

        let report = scan_data_path(dir.path()).unwrap();

        assert_eq!(report.summary.entry_count, 1);
        assert_eq!(report.buckets[0].entries[0].name, "entry");
    }

    #[test]
    fn doctor_reports_bucket_without_settings() {
        let dir = tempdir().unwrap();
        fs::create_dir_all(dir.path().join("bucket-1")).unwrap();

        let report = scan_data_path(dir.path()).unwrap();

        assert_eq!(report.summary.bucket_count, 1);
        assert_eq!(report.summary.error_count, 1);
        assert_eq!(
            report.issues[0].kind,
            DoctorIssueKind::UnreadableBucketSettings
        );
    }

    #[test]
    fn doctor_reports_corrupt_block_index() {
        let dir = tempdir().unwrap();
        let bucket = dir.path().join("bucket-1");
        let entry = bucket.join("entry");
        fs::create_dir_all(&entry).unwrap();
        fs::write(bucket.join(BUCKET_SETTINGS_FILE), []).unwrap();
        fs::write(entry.join(BLOCK_INDEX_FILE), b"not a valid index").unwrap();

        let report = scan_data_path(dir.path()).unwrap();

        let corrupt = report
            .issues
            .iter()
            .find(|issue| issue.kind == DoctorIssueKind::CorruptBlockIndex)
            .expect("expected CorruptBlockIndex issue");
        assert_eq!(corrupt.severity, DoctorSeverity::Error);
    }

    #[test]
    fn doctor_reports_empty_block_index_when_descriptors_exist() {
        let dir = tempdir().unwrap();
        let bucket = dir.path().join("bucket-1");
        let entry = bucket.join("entry");
        fs::create_dir_all(&entry).unwrap();
        fs::write(bucket.join(BUCKET_SETTINGS_FILE), []).unwrap();
        fs::write(entry.join(BLOCK_INDEX_FILE), []).unwrap();
        fs::write(entry.join("1000.meta"), []).unwrap();

        let report = scan_data_path(dir.path()).unwrap();

        let corrupt = report
            .issues
            .iter()
            .find(|issue| issue.kind == DoctorIssueKind::CorruptBlockIndex)
            .expect("expected CorruptBlockIndex issue");
        assert!(corrupt.message.contains("empty"));
    }

    #[test]
    fn doctor_reports_invalid_bucket_settings() {
        let dir = tempdir().unwrap();
        let bucket = dir.path().join("bucket-1");
        fs::create_dir_all(&bucket).unwrap();
        fs::write(
            bucket.join(BUCKET_SETTINGS_FILE),
            b"not a valid settings proto",
        )
        .unwrap();

        let report = scan_data_path(dir.path()).unwrap();

        assert!(report
            .issues
            .iter()
            .any(|i| i.kind == DoctorIssueKind::InvalidBucketSettings));
    }

    #[test]
    fn doctor_reports_missing_descriptor_and_data_block() {
        let dir = tempdir().unwrap();
        let bucket = dir.path().join("bucket-1");
        let entry = bucket.join("entry");
        fs::create_dir_all(&entry).unwrap();
        fs::write(bucket.join(BUCKET_SETTINGS_FILE), []).unwrap();
        let index_bytes = build_index_bytes(&[(1000, 0, 0, 0, None)]);
        fs::write(entry.join(BLOCK_INDEX_FILE), index_bytes).unwrap();

        let report = scan_data_path(dir.path()).unwrap();

        assert!(report
            .issues
            .iter()
            .any(|i| i.kind == DoctorIssueKind::IndexReferencesMissingDescriptor));
        assert!(report
            .issues
            .iter()
            .any(|i| i.kind == DoctorIssueKind::IndexReferencesMissingDataBlock));
    }

    #[test]
    fn doctor_reports_orphan_descriptor_and_data_block() {
        let dir = tempdir().unwrap();
        let bucket = dir.path().join("bucket-1");
        let entry = bucket.join("entry");
        fs::create_dir_all(&entry).unwrap();
        fs::write(bucket.join(BUCKET_SETTINGS_FILE), []).unwrap();
        let index_bytes = build_index_bytes(&[(1000, 0, 0, 0, None)]);
        fs::write(entry.join(BLOCK_INDEX_FILE), index_bytes).unwrap();
        fs::write(entry.join("2000.meta"), []).unwrap();
        fs::write(entry.join("2000.blk"), []).unwrap();

        let report = scan_data_path(dir.path()).unwrap();

        assert!(report
            .issues
            .iter()
            .any(|i| i.kind == DoctorIssueKind::DescriptorNotIndexed));
        assert!(report
            .issues
            .iter()
            .any(|i| i.kind == DoctorIssueKind::DataBlockNotIndexed));
    }

    #[test]
    fn doctor_reports_descriptor_crc_mismatch() {
        let dir = tempdir().unwrap();
        let bucket = dir.path().join("bucket-1");
        let entry = bucket.join("entry");
        fs::create_dir_all(&entry).unwrap();
        fs::write(bucket.join(BUCKET_SETTINGS_FILE), []).unwrap();

        let descriptor = build_minimal_block(1000, 8);
        let mut crc = Digest::new();
        crc.write(&descriptor);
        let wrong_crc = crc.sum64() ^ 0x1;
        let index_bytes =
            build_index_bytes(&[(1000, descriptor.len() as u64, 0, 0, Some(wrong_crc))]);
        fs::write(entry.join(BLOCK_INDEX_FILE), index_bytes).unwrap();
        fs::write(entry.join("1000.meta"), descriptor).unwrap();
        fs::write(entry.join("1000.blk"), vec![0u8; 16]).unwrap();

        let report = scan_data_path(dir.path()).unwrap();

        assert!(report
            .issues
            .iter()
            .any(|i| i.kind == DoctorIssueKind::DescriptorCrcMismatch));
    }

    #[test]
    fn doctor_reports_data_block_too_small() {
        let dir = tempdir().unwrap();
        let bucket = dir.path().join("bucket-1");
        let entry = bucket.join("entry");
        fs::create_dir_all(&entry).unwrap();
        fs::write(bucket.join(BUCKET_SETTINGS_FILE), []).unwrap();

        let descriptor = build_minimal_block(1000, 32);
        let mut crc = Digest::new();
        crc.write(&descriptor);
        let descriptor_crc = crc.sum64();
        let index_bytes =
            build_index_bytes(&[(1000, descriptor.len() as u64, 0, 0, Some(descriptor_crc))]);
        fs::write(entry.join(BLOCK_INDEX_FILE), index_bytes).unwrap();
        fs::write(entry.join("1000.meta"), descriptor).unwrap();
        fs::write(entry.join("1000.blk"), vec![0u8; 4]).unwrap();

        let report = scan_data_path(dir.path()).unwrap();

        assert!(report
            .issues
            .iter()
            .any(|i| i.kind == DoctorIssueKind::DataBlockTooSmall));
    }

    #[test]
    fn doctor_accepts_valid_index_with_matching_files() {
        let dir = tempdir().unwrap();
        let bucket = dir.path().join("bucket-1");
        let entry = bucket.join("entry");
        fs::create_dir_all(&entry).unwrap();
        fs::write(bucket.join(BUCKET_SETTINGS_FILE), []).unwrap();

        let descriptor = build_minimal_block(1000, 8);
        let mut crc = Digest::new();
        crc.write(&descriptor);
        let descriptor_crc = crc.sum64();
        let index_bytes =
            build_index_bytes(&[(1000, descriptor.len() as u64, 0, 0, Some(descriptor_crc))]);
        fs::write(entry.join(BLOCK_INDEX_FILE), index_bytes).unwrap();
        fs::write(entry.join("1000.meta"), descriptor).unwrap();
        fs::write(entry.join("1000.blk"), vec![0u8; 16]).unwrap();

        let report = scan_data_path(dir.path()).unwrap();

        assert!(
            report.issues.is_empty(),
            "expected no issues, got: {:?}",
            report.issues
        );
        assert_eq!(report.summary.block_count, 1);
    }
}
