// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::core::duration::parse_duration_to_micros;
use crate::lifecycle::action::LifecycleContext;
use crate::storage::engine::StorageEngine;
use crate::syslog::{SYSTEM_BUCKET_NAME, SYSTEM_LIFECYCLE_ENTRY_PREFIX};
use reduct_base::error::ReductError;
use reduct_base::io::ReadRecord;
use reduct_base::msg::lifecycle_api::LifecycleSettings;
use std::sync::Arc;

pub(crate) struct ProcessingWindow {
    pub(crate) start: Option<u64>,
    pub(crate) stop: Option<u64>,
    pub(crate) last_processed_ts: Option<u64>,
    pub(crate) reaches_cutoff: bool,
}

pub(crate) async fn processing_window(
    settings: &LifecycleSettings,
    context: &LifecycleContext,
    policy_name: &str,
    cutoff_stop: u64,
) -> Result<ProcessingWindow, ReductError> {
    let (first_record_start, effective_cutoff_stop) =
        matching_record_window(settings, context, cutoff_stop).await?;

    if !context.system_events_enabled {
        let stop = effective_cutoff_stop;
        return Ok(ProcessingWindow {
            start: Some(first_record_start.min(stop)),
            stop: Some(stop),
            last_processed_ts: None,
            reaches_cutoff: false,
        });
    }

    let interval_us = parse_duration_to_micros(&settings.interval)?.max(0) as u64;
    let data_window = interval_us.saturating_mul(24);
    let last_processed = read_progress(
        &context.storage,
        &context.system_event_instance,
        policy_name,
    )
    .await
    .unwrap_or(first_record_start);

    if last_processed >= effective_cutoff_stop {
        let stop = effective_cutoff_stop;
        return Ok(ProcessingWindow {
            start: Some(first_record_start.min(stop)),
            stop: Some(stop),
            last_processed_ts: Some(last_processed),
            reaches_cutoff: true,
        });
    }

    let window_stop = last_processed
        .saturating_add(data_window)
        .min(effective_cutoff_stop);
    Ok(ProcessingWindow {
        start: Some(first_record_start.min(window_stop)),
        stop: Some(window_stop),
        last_processed_ts: Some(window_stop),
        reaches_cutoff: window_stop >= effective_cutoff_stop,
    })
}

async fn matching_record_window(
    settings: &LifecycleSettings,
    context: &LifecycleContext,
    cutoff_stop: u64,
) -> Result<(u64, u64), ReductError> {
    let bucket = context
        .storage
        .get_bucket(&settings.bucket)
        .await?
        .upgrade()?;
    let bucket_info = bucket.info().await?;
    let requested_entries = requested_entries(&settings.entries);
    let mut matching_records = bucket_info
        .entries
        .into_iter()
        .filter(|entry| entry.record_count > 0)
        .filter(|entry| is_requested_entry(&entry.name, &requested_entries))
        .collect::<Vec<_>>();

    if matching_records.is_empty() {
        return Ok((cutoff_stop, cutoff_stop));
    }

    let first_record_start = matching_records
        .iter()
        .map(|entry| entry.oldest_record)
        .min()
        .unwrap_or(cutoff_stop)
        .min(cutoff_stop);
    let effective_cutoff_stop = matching_records
        .drain(..)
        .map(|entry| entry.latest_record.saturating_add(1))
        .max()
        .unwrap_or(cutoff_stop)
        .min(cutoff_stop);

    Ok((first_record_start, effective_cutoff_stop))
}

fn requested_entries(entries: &[String]) -> Option<&[String]> {
    if entries.is_empty() || entries.iter().any(|entry| entry == "*") {
        None
    } else {
        Some(entries)
    }
}

fn is_requested_entry(entry_name: &str, requested_entries: &Option<&[String]>) -> bool {
    requested_entries
        .map(|patterns| entry_matches_patterns(entry_name, patterns))
        .unwrap_or(true)
}

fn entry_matches_patterns(entry_name: &str, patterns: &[String]) -> bool {
    patterns.iter().any(|pattern| {
        if let Some(prefix) = pattern.strip_suffix('*') {
            entry_name.starts_with(prefix)
        } else {
            entry_name == pattern
        }
    })
}

pub(crate) async fn read_progress(
    storage: &StorageEngine,
    instance_name: &str,
    policy_name: &str,
) -> Option<u64> {
    let entry_path = lifecycle_stats_entry_path(instance_name, policy_name);
    let bucket = storage
        .get_bucket(SYSTEM_BUCKET_NAME)
        .await
        .ok()?
        .upgrade()
        .ok()?;
    let latest_record = Arc::clone(&bucket)
        .info()
        .await
        .ok()?
        .entries
        .into_iter()
        .find(|entry| entry.name == entry_path)?
        .latest_record;

    let entry = bucket.get_entry(&entry_path).await.ok()?.upgrade().ok()?;
    let mut reader = entry.begin_read(latest_record).await.ok()?;
    let record = reader.read_chunk()?.ok()?;
    serde_json::from_slice::<serde_json::Value>(&record)
        .ok()?
        .get("last_processed_ts")?
        .as_u64()
}

fn lifecycle_stats_entry_path(instance_name: &str, policy_name: &str) -> String {
    let instance = if instance_name.is_empty() {
        "unknown"
    } else {
        instance_name
    };
    format!(
        "{}/{}/{}",
        SYSTEM_LIFECYCLE_ENTRY_PREFIX,
        instance,
        sanitize_policy_name(policy_name)
    )
}

fn sanitize_policy_name(name: &str) -> String {
    name.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::lifecycle::action::LifecycleContext;
    use crate::lifecycle::lifecycle_task::tests::{settings_fixture, storage};
    use crate::storage::bucket::tests::write;
    use reduct_base::msg::bucket_api::BucketSettings;

    #[tokio::test]
    async fn read_progress_returns_none_when_no_system_bucket() {
        let storage = storage().await;

        assert_eq!(
            read_progress(&storage, "instance-1", "policy-1").await,
            None
        );
    }

    #[tokio::test]
    async fn read_progress_returns_none_when_no_entry() {
        let storage = storage().await;
        storage
            .create_system_bucket(SYSTEM_BUCKET_NAME, BucketSettings::default())
            .await
            .unwrap();

        assert_eq!(
            read_progress(&storage, "instance-1", "policy-1").await,
            None
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn read_progress_reads_latest_lifecycle_stats() {
        let storage = storage().await;

        write_lifecycle_stats(&storage, "instance-1", "policy/1", 12345)
            .await
            .unwrap();

        assert_eq!(
            read_progress(&storage, "instance-1", "policy/1").await,
            Some(12345)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn read_progress_uses_latest_lifecycle_stats_record() {
        let storage = storage().await;

        write_lifecycle_stats(&storage, "instance-1", "policy-1", 12345)
            .await
            .unwrap();
        write_lifecycle_stats(&storage, "instance-1", "policy-1", 67890)
            .await
            .unwrap();

        assert_eq!(
            read_progress(&storage, "instance-1", "policy-1").await,
            Some(67890)
        );
    }

    #[tokio::test]
    async fn processing_window_uses_oldest_matching_record_as_start() {
        let storage = storage().await;
        let bucket = storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        let mut settings = settings_fixture();
        settings.entries = vec!["entry-a*".to_string()];
        write(&bucket, "entry-a", 50, b"a").await.unwrap();
        write(&bucket, "entry-b", 10, b"b").await.unwrap();

        let window = processing_window(
            &settings,
            &LifecycleContext::new(storage, true, "instance-1".to_string()),
            "policy-1",
            100,
        )
        .await
        .unwrap();

        assert_eq!(window.start, Some(50));
        assert_eq!(window.stop, Some(51));
        assert_eq!(window.last_processed_ts, Some(51));
        assert!(window.reaches_cutoff);
    }

    #[tokio::test]
    async fn processing_window_clamps_start_when_all_matching_records_are_newer_than_stop() {
        let storage = storage().await;
        let bucket = storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        let settings = settings_fixture();
        write(&bucket, "entry-1", 150, b"fresh").await.unwrap();

        let window = processing_window(
            &settings,
            &LifecycleContext::new(storage, true, "instance-1".to_string()),
            "policy-1",
            100,
        )
        .await
        .unwrap();

        assert_eq!(window.start, Some(100));
        assert_eq!(window.stop, Some(100));
        assert_eq!(window.last_processed_ts, Some(100));
        assert!(window.reaches_cutoff);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn processing_window_clamps_start_to_window_stop_when_progress_lags_behind_data() {
        let storage = storage().await;
        let bucket = storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        let mut settings = settings_fixture();
        settings.interval = "0s".to_string();
        write_lifecycle_stats(&storage, "instance-1", "policy-1", 1)
            .await
            .unwrap();
        write(&bucket, "entry-1", 100, b"fresh").await.unwrap();

        let window = processing_window(
            &settings,
            &LifecycleContext::new(storage, true, "instance-1".to_string()),
            "policy-1",
            1_000,
        )
        .await
        .unwrap();

        assert_eq!(window.start, Some(1));
        assert_eq!(window.stop, Some(1));
        assert_eq!(window.last_processed_ts, Some(1));
        assert!(!window.reaches_cutoff);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn processing_window_is_caught_up_when_progress_is_past_latest_matching_record() {
        let storage = storage().await;
        let bucket = storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade()
            .unwrap();
        let settings = settings_fixture();
        write(&bucket, "entry-1", 100, b"old").await.unwrap();
        write_lifecycle_stats(&storage, "instance-1", "policy-1", 101)
            .await
            .unwrap();

        let window = processing_window(
            &settings,
            &LifecycleContext::new(storage, true, "instance-1".to_string()),
            "policy-1",
            u64::MAX,
        )
        .await
        .unwrap();

        assert_eq!(window.start, Some(100));
        assert_eq!(window.stop, Some(101));
        assert_eq!(window.last_processed_ts, Some(101));
        assert!(window.reaches_cutoff);
    }

    pub(crate) async fn write_lifecycle_stats(
        storage: &StorageEngine,
        instance_name: &str,
        policy_name: &str,
        last_processed_ts: u64,
    ) -> Result<(), reduct_base::error::ReductError> {
        use bytes::Bytes;
        use reduct_base::Labels;
        use std::time::{SystemTime, UNIX_EPOCH};

        let payload = serde_json::json!({
            "policy_name": policy_name,
            "action_type": "delete",
            "bucket": "bucket-1",
            "duration": 0.1,
            "processed_records": 1,
            "last_processed_ts": last_processed_ts,
        })
        .to_string()
        .into_bytes();
        storage
            .create_system_bucket(SYSTEM_BUCKET_NAME, BucketSettings::default())
            .await
            .ok();
        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        let mut writer = storage
            .begin_write(
                SYSTEM_BUCKET_NAME,
                &lifecycle_stats_entry_path(instance_name, policy_name),
                now_us,
                payload.len() as u64,
                "application/json".to_string(),
                Labels::default(),
            )
            .await?;
        writer.send(Ok(Some(Bytes::from(payload)))).await?;
        writer.send(Ok(None)).await?;
        Ok(())
    }
}
