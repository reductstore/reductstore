// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::bucket::BucketSettingsAxum;
use crate::api::StateKeeper;
use crate::api::{Components, HttpError};
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
        .create_bucket(&bucket_name, settings.into())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::api::tests::{components, headers};
    use crate::api::Components;

    use rstest::rstest;

    use reduct_base::error::ErrorCode;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket(#[future] components: Arc<Components>, headers: HeaderMap) {
        create_bucket(
            State(components.await),
            Path("bucket-3".to_string()),
            headers,
            BucketSettingsAxum::default(),
        )
        .await
        .unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket_already_exists(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
    ) {
        let err = create_bucket(
            State(components.await),
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
