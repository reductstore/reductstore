// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::auth::policy::FullAccessPolicy;
use crate::http_frontend::bucket_api::BucketSettingsAxum;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::{Componentes, HttpError};
use axum::extract::{Path, State};
use axum::headers::HeaderMap;
use std::sync::Arc;

// PUT /b/:bucket_name
pub async fn update_bucket(
    State(components): State<Arc<Componentes>>,
    Path(bucket_name): Path<String>,
    headers: HeaderMap,
    settings: BucketSettingsAxum,
) -> Result<(), HttpError> {
    check_permissions(&components, headers, FullAccessPolicy {}).await?;
    let mut storage = components.storage.write().await;

    Ok(storage
        .get_mut_bucket(&bucket_name)?
        .set_settings(settings.into())?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_frontend::tests::{components, headers};
    use reduct_base::error::ErrorCode;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_update_bucket(components: Arc<Componentes>, headers: HeaderMap) {
        update_bucket(
            State(components),
            Path("bucket-1".to_string()),
            headers,
            BucketSettingsAxum::default(),
        )
        .await
        .unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_bucket_not_found(components: Arc<Componentes>, headers: HeaderMap) {
        let err = update_bucket(
            State(components),
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
