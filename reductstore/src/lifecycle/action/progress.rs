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
    pub(crate) caught_up: bool,
}

pub(crate) async fn processing_window(
    settings: &LifecycleSettings,
    context: &LifecycleContext,
    policy_name: &str,
    cutoff_stop: u64,
) -> Result<ProcessingWindow, ReductError> {
    if !context.system_events_enabled {
        return Ok(ProcessingWindow {
            start: Some(0),
            stop: Some(cutoff_stop),
            last_processed_ts: None,
            caught_up: false,
        });
    }

    let interval_us = parse_duration_to_micros(&settings.interval)?.max(0) as u64;
    let data_window = interval_us.saturating_mul(24);
    let window_start = read_progress(
        &context.storage,
        &context.system_event_instance,
        policy_name,
    )
    .await
    .unwrap_or(0);

    if window_start >= cutoff_stop {
        return Ok(ProcessingWindow {
            start: Some(window_start),
            stop: Some(window_start),
            last_processed_ts: Some(window_start),
            caught_up: true,
        });
    }

    let window_stop = window_start.saturating_add(data_window).min(cutoff_stop);
    Ok(ProcessingWindow {
        start: Some(window_start),
        stop: Some(window_stop),
        last_processed_ts: Some(window_stop),
        caught_up: false,
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
    use crate::lifecycle::lifecycle_task::tests::storage;
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
