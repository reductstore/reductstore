// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::bucket::BucketSettingsAxum;
use crate::api::http::HttpError;
use crate::api::http::StateKeeper;
use crate::auth::policy::FullAccessPolicy;

use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// POST /b/:bucket_name
pub(super) async fn create_bucket(
    State(keeper): State<Arc<StateKeeper>>,
    Path(bucket_name): Path<String>,
    headers: HeaderMap,
    settings: BucketSettingsAxum,
) -> Result<(), HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;
    components
        .storage
        .create_bucket(&bucket_name, settings.into())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::api::http::tests::{headers, keeper, keeper_with_cfg};
    use crate::cfg::Cfg;
    use reduct_base::error::ErrorCode;
    use reduct_base::msg::bucket_api::{BucketSettings, QuotaType};
    use rstest::rstest;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        create_bucket(
            State(keeper.await),
            Path("bucket-3".to_string()),
            headers,
            BucketSettingsAxum::default(),
        )
        .await
        .unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket_uses_configured_defaults(headers: HeaderMap) {
        let bucket_defaults = BucketSettings {
            max_block_size: Some(1_000_000),
            quota_type: Some(QuotaType::FIFO),
            quota_size: Some(10_000_000),
            max_block_records: Some(10),
        };
        let keeper = keeper_with_cfg(Cfg {
            data_path: tempfile::tempdir().unwrap().keep(),
            bucket_defaults: bucket_defaults.clone(),
            ..Cfg::default()
        })
        .await;

        create_bucket(
            State(Arc::clone(&keeper)),
            Path("bucket-3".to_string()),
            headers,
            BucketSettingsAxum::default(),
        )
        .await
        .unwrap();

        let bucket = keeper
            .get_anonymous()
            .await
            .unwrap()
            .storage
            .get_bucket("bucket-3")
            .await
            .unwrap()
            .upgrade_and_unwrap();

        assert_eq!(bucket.settings().await.unwrap(), bucket_defaults);
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket_fills_partial_settings_from_configured_defaults(
        headers: HeaderMap,
    ) {
        let bucket_defaults = BucketSettings {
            max_block_size: Some(1_000_000),
            quota_type: Some(QuotaType::FIFO),
            quota_size: Some(10_000_000),
            max_block_records: Some(10),
        };
        let keeper = keeper_with_cfg(Cfg {
            data_path: tempfile::tempdir().unwrap().keep(),
            bucket_defaults: bucket_defaults.clone(),
            ..Cfg::default()
        })
        .await;

        create_bucket(
            State(Arc::clone(&keeper)),
            Path("bucket-3".to_string()),
            headers,
            BucketSettingsAxum(BucketSettings {
                max_block_size: Some(2_000_000),
                ..BucketSettings::default()
            }),
        )
        .await
        .unwrap();

        let bucket = keeper
            .get_anonymous()
            .await
            .unwrap()
            .storage
            .get_bucket("bucket-3")
            .await
            .unwrap()
            .upgrade_and_unwrap();

        assert_eq!(
            bucket.settings().await.unwrap(),
            BucketSettings {
                max_block_size: Some(2_000_000),
                ..bucket_defaults
            }
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket_already_exists(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let err = create_bucket(
            State(keeper.await),
            Path("bucket-1".to_string()),
            headers,
            BucketSettingsAxum::default(),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(
            err,
            HttpError::new(ErrorCode::Conflict, "Bucket 'bucket-1' already exists",)
        )
    }
}
