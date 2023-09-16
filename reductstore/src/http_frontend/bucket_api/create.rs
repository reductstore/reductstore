// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::auth::policy::FullAccessPolicy;
use crate::http_frontend::bucket_api::BucketSettingsAxum;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::{Componentes, HttpError};

use axum::extract::{Path, State};
use axum::headers::HeaderMap;
use std::sync::Arc;

// POST /b/:bucket_name
pub async fn create_bucket(
    State(components): State<Arc<Componentes>>,
    Path(bucket_name): Path<String>,
    headers: HeaderMap,
    settings: BucketSettingsAxum,
) -> Result<(), HttpError> {
    check_permissions(&components, headers, FullAccessPolicy {}).await?;
    components
        .storage
        .write()
        .await
        .create_bucket(&bucket_name, settings.into())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::http_frontend::tests::{components, headers};
    use crate::http_frontend::Componentes;

    use rstest::rstest;

    use reduct_base::error::ErrorCode;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket(components: Arc<Componentes>, headers: HeaderMap) {
        create_bucket(
            State(components),
            Path("bucket-3".to_string()),
            headers,
            BucketSettingsAxum::default(),
        )
        .await
        .unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_bucket_already_exists(components: Arc<Componentes>, headers: HeaderMap) {
        let err = create_bucket(
            State(components),
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
