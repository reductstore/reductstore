// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::{Components, HttpError};
use crate::auth::policy::FullAccessPolicy;

use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;

use std::sync::Arc;

// DELETE /b/:bucket_name
pub(crate) async fn remove_bucket(
    State(components): State<Arc<Components>>,
    Path(bucket_name): Path<String>,
    headers: HeaderMap,
) -> Result<(), HttpError> {
    check_permissions(&components, headers, FullAccessPolicy {}).await?;
    components.storage.remove_bucket(&bucket_name).await?;
    components
        .token_repo
        .write()
        .await
        .remove_bucket_from_tokens(&bucket_name)?;
    Ok(())
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
    async fn test_remove_bucket(#[future] components: Arc<Components>, headers: HeaderMap) {
        remove_bucket(
            State(components.await),
            Path("bucket-1".to_string()),
            headers,
        )
        .await
        .unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_bucket_not_found(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
    ) {
        let err = remove_bucket(
            State(components.await),
            Path("not-found".to_string()),
            headers,
        )
        .await
        .err()
        .unwrap();
        assert_eq!(
            err,
            HttpError::new(ErrorCode::NotFound, "Bucket 'not-found' is not found",)
        )
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_bucket_from_permission(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
    ) {
        let components = components.await;
        let token = components
            .token_repo
            .read()
            .await
            .get_token("test")
            .unwrap()
            .clone();
        assert_eq!(
            token.permissions.unwrap().read,
            vec!["bucket-1".to_string(), "bucket-2".to_string()]
        );

        remove_bucket(
            State(components.clone()),
            Path("bucket-1".to_string()),
            headers.clone(),
        )
        .await
        .unwrap();

        let token = components
            .token_repo
            .read()
            .await
            .get_token("test")
            .unwrap()
            .clone();
        assert_eq!(
            token.permissions.unwrap().read,
            vec!["bucket-2".to_string()]
        );
    }
}
