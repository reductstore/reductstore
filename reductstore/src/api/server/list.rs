// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::server::BucketInfoListAxum;
use crate::api::{Components, HttpError};
use crate::auth::policy::{AuthenticatedPolicy, ReadAccessPolicy};
use axum::extract::State;
use axum_extra::headers::HeaderMap;
use reduct_base::msg::server_api::BucketInfoList;
use std::sync::Arc;

// GET /list
pub(crate) async fn list(
    State(components): State<Arc<Components>>,
    headers: HeaderMap,
) -> Result<BucketInfoListAxum, HttpError> {
    check_permissions(&components, &headers, AuthenticatedPolicy {}).await?;

    let mut filtered_by_read_permission = vec![];

    for bucket in components.storage.get_bucket_list()?.buckets {
        // Filter out buckets that are not visible to the user
        if check_permissions(
            &components,
            &headers,
            ReadAccessPolicy {
                bucket: &bucket.name,
            },
        )
        .await
        .is_ok()
        {
            filtered_by_read_permission.push(bucket);
        }
    }

    Ok(BucketInfoList {
        buckets: filtered_by_read_permission,
    }
    .into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{components, headers};
    use axum::http::HeaderValue;
    use reduct_base::msg::token_api::Permissions;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_list(#[future] components: Arc<Components>, headers: HeaderMap) {
        let list = list(State(components.await), headers).await.unwrap();
        assert_eq!(list.0.buckets.len(), 2);
    }

    #[rstest]
    #[tokio::test]
    async fn test_filtered_list(#[future] components: Arc<Components>, mut headers: HeaderMap) {
        let components = components.await;
        let token = components
            .token_repo
            .write()
            .await
            .generate_token(
                "with-one-bucket",
                Permissions {
                    read: vec!["bucket-1".to_string()],
                    ..Default::default()
                },
            )
            .unwrap();

        headers.insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {}", token.value)).unwrap(),
        );
        let list = list(State(components), headers).await.unwrap();
        assert_eq!(list.0.buckets.len(), 1);
        assert_eq!(list.0.buckets[0].name, "bucket-1");
    }
}
