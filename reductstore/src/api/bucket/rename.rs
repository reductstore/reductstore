// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::HttpError;
use crate::api::StateKeeper;
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum::Json;
use axum_extra::headers::HeaderMap;
use reduct_base::msg::bucket_api::RenameBucket;
use std::sync::Arc;

// PUT /b/:bucket_name/rename
pub(super) async fn rename_bucket(
    State(keeper): State<Arc<StateKeeper>>,
    Path(bucket_name): Path<String>,
    headers: HeaderMap,
    request: Json<RenameBucket>,
) -> Result<(), HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;
    components
        .storage
        .rename_bucket(bucket_name.clone(), request.new_name.clone())
        .await?;
    components
        .token_repo
        .write()
        .await?
        .rename_bucket(&bucket_name, &request.new_name)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{headers, keeper};
    use axum::Json;
    use reduct_base::error::ReductError;
    use reduct_base::msg::token_api::Permissions;
    use reduct_base::not_found;
    use rstest::{fixture, rstest};
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_rename_bucket(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        request: Json<RenameBucket>,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        components
            .token_repo
            .write()
            .await
            .unwrap()
            .generate_token(
                "test-1",
                Permissions {
                    full_access: false,
                    read: vec!["bucket-1".to_string()],
                    write: vec!["bucket-1".to_string()],
                },
            )
            .unwrap();

        rename_bucket(
            State(keeper.clone()),
            Path("bucket-1".to_string()),
            headers,
            request,
        )
        .await
        .unwrap();

        let components = keeper.get_anonymous().await.unwrap();
        let bucket = components
            .storage
            .get_bucket("new-bucket")
            .await
            .unwrap()
            .upgrade()
            .unwrap();

        assert_eq!(bucket.name(), "new-bucket");

        let token = components
            .token_repo
            .write()
            .await
            .unwrap()
            .get_token("test-1")
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
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        request: Json<RenameBucket>,
    ) {
        let keeper = keeper.await;
        let result = rename_bucket(
            State(keeper.clone()),
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
