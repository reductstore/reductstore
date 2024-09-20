// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::bucket::FullBucketInfoAxum;
use crate::api::middleware::check_permissions;
use crate::api::Components;
use crate::api::HttpError;
use crate::auth::policy::AuthenticatedPolicy;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// GET /b/:bucket_name

pub(crate) async fn get_bucket(
    State(components): State<Arc<Components>>,
    Path(bucket_name): Path<String>,
    headers: HeaderMap,
) -> Result<FullBucketInfoAxum, HttpError> {
    check_permissions(&components, headers, AuthenticatedPolicy {}).await?;
    let bucket_info = components.storage.get_bucket(&bucket_name)?.upgrade()?;
    Ok(bucket_info.info()?.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::api::Components;

    use crate::api::tests::{components, headers};

    use rstest::rstest;

    use reduct_base::error::ErrorCode;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_get_bucket(#[future] components: Arc<Components>, headers: HeaderMap) {
        let info = get_bucket(
            State(components.await),
            Path("bucket-1".to_string()),
            headers,
        )
        .await
        .unwrap();
        assert_eq!(info.0.info.name, "bucket-1");
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_bucket_not_found(#[future] components: Arc<Components>, headers: HeaderMap) {
        let err = get_bucket(
            State(components.await),
            Path("not-found".to_string()),
            headers,
        )
        .await
        .err()
        .unwrap();
        assert_eq!(
            err,
            HttpError::new(ErrorCode::NotFound, "Bucket 'not-found' is not found")
        )
    }
}
