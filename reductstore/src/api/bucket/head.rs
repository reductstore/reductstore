// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::{Components, HttpError};
use crate::auth::policy::AuthenticatedPolicy;

use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// HEAD /b/:bucket_name
pub(crate) async fn head_bucket(
    State(components): State<Arc<Components>>,
    Path(bucket_name): Path<String>,
    headers: HeaderMap,
) -> Result<(), HttpError> {
    check_permissions(&components, headers, AuthenticatedPolicy {}).await?;
    components.storage.read().await.get_bucket(&bucket_name)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::api::Components;

    use crate::api::tests::{components, headers};

    use rstest::rstest;

    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_head_bucket(#[future] components: Arc<Components>, headers: HeaderMap) {
        head_bucket(
            State(components.await),
            Path("bucket-1".to_string()),
            headers,
        )
        .await
        .unwrap();
    }
}
