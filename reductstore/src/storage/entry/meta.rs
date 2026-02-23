// Copyright 2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

/// Reserved segment for system metadata entries.
///
/// Rules:
/// - Metadata for entry `<entry>` is stored in `<entry>/$meta`.
/// - `$meta` entries are system entries and cannot be removed directly.
/// - `$meta` entries are included in storage usage/size accounting.
/// - `$meta` entries are excluded from user-facing entry counts/lists.
/// - FIFO quota eviction must ignore `$meta` entries as removal candidates.
/// - Every `$meta` record must include label `key`.
/// - `$meta` behaves as upsert-by-key: writing a record with `key=K` removes existing `$meta`
///   records with the same key before adding the new one.
pub(crate) const META_ENTRY_SEGMENT: &str = "$meta";

/// Max block size for system metadata entries.
pub(crate) const META_ENTRY_MAX_BLOCK_SIZE: u64 = 128 * 1024;

pub(crate) fn is_system_meta_entry(entry_name: &str) -> bool {
    entry_name == META_ENTRY_SEGMENT || entry_name.ends_with(&format!("/{}", META_ENTRY_SEGMENT))
}

pub(crate) fn meta_entry_parent(entry_name: &str) -> Option<&str> {
    entry_name
        .strip_suffix(&format!("/{}", META_ENTRY_SEGMENT))
        .filter(|base| !base.is_empty())
}

pub(crate) fn meta_entry_name(entry_name: &str) -> String {
    format!("{entry_name}/{META_ENTRY_SEGMENT}")
}
