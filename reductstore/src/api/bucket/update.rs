// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::bucket::BucketSettingsAxum;
use crate::api::StateKeeper;
use crate::api::{Components, HttpError};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// PUT /b/:bucket_name
pub(super) async fn update_bucket(
    State(keeper): State<Arc<StateKeeper>>,
    Path(bucket_name): Path<String>,
    headers: HeaderMap,
    settings: BucketSettingsAxum,
) -> Result<(), HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;
    Ok(components
        .storage
        .get_bucket(&bucket_name)?
        .upgrade()?
        .set_settings(settings.into())
        .await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{components, headers};
    use reduct_base::error::ErrorCode;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_update_bucket(#[future] components: Arc<Components>, headers: HeaderMap) {
        update_bucket(
            State(components.await),
            Path("bucket-1".to_string()),
            headers,
            BucketSettingsAxum::default(),
        )
        .await
        .unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_bucket_not_found(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
    ) {
        let err = update_bucket(
            State(components.await),
            Path("not-found".to_string()),
            headers,
            BucketSettingsAxum::default(),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(
            err,
            HttpError::new(ErrorCode::NotFound, "Bucket 'not-found' is not found",)
        )
    }
}
