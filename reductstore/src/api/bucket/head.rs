// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::StateKeeper;
use crate::api::{Components, HttpError};
use crate::auth::policy::AuthenticatedPolicy;

use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// HEAD /b/:bucket_name
pub(super) async fn head_bucket(
    State(keeper): State<Arc<StateKeeper>>,
    Path(bucket_name): Path<String>,
    headers: HeaderMap,
) -> Result<(), HttpError> {
    let components = keeper
        .get_with_permissions(&headers, AuthenticatedPolicy {})
        .await?;
    components.storage.get_bucket(&bucket_name)?;
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
