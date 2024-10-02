// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::{Components, HttpError};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum::Json;
use axum_extra::headers::HeaderMap;
use reduct_base::msg::bucket_api::RenameBucket;
use std::sync::Arc;

// PUT /b/:bucket_name/rename
pub(crate) async fn rename_bucket(
    State(components): State<Arc<Components>>,
    Path(bucket_name): Path<String>,
    headers: HeaderMap,
    request: Json<RenameBucket>,
) -> Result<(), HttpError> {
    check_permissions(&components, headers, FullAccessPolicy {}).await?;
    components
        .storage
        .rename_bucket(&bucket_name, &request.new_name)
        .await?;
    components
        .token_repo
        .write()
        .await
        .rename_bucket(&bucket_name, &request.new_name)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{components, headers};
    use crate::api::Components;
    use reduct_base::error::ReductError;
    use reduct_base::not_found;
    use rstest::{fixture, rstest};
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_rename_bucket(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
        request: Json<RenameBucket>,
    ) {
        let components = components.await;
        rename_bucket(
            State(components.clone()),
            Path("bucket-1".to_string()),
            headers,
            request,
        )
        .await
        .unwrap();

        let bucket = components
            .storage
            .get_bucket("new-bucket")
            .unwrap()
            .upgrade()
            .unwrap();

        assert_eq!(bucket.name(), "new-bucket");

        let token = components
            .token_repo
            .read()
            .await
            .get_token("test")
            .unwrap()
            .clone();
        assert_eq!(
            token.permissions.unwrap().write,
            vec!["new-bucket".to_string()]
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_rename_bucket_not_found(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
        request: Json<RenameBucket>,
    ) {
        let components = components.await;
        let result = rename_bucket(
            State(components.clone()),
            Path("NOT_EXIST".to_string()),
            headers,
            request,
        )
        .await;

        assert_eq!(
            result.unwrap_err(),
            not_found!("Bucket 'NOT_EXIST' is not found").into()
        );
    }

    #[fixture]
    fn request() -> Json<RenameBucket> {
        Json(RenameBucket {
            new_name: "new-bucket".to_string(),
        })
    }
}
