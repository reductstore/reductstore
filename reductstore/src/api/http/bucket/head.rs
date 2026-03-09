// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::http::HttpError;
use crate::api::http::StateKeeper;
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
    components.storage.get_bucket(&bucket_name).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::api::http::tests::{headers, keeper};
    use rstest::rstest;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_head_bucket(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        head_bucket(State(keeper.await), Path("bucket-1".to_string()), headers)
            .await
            .unwrap();
    }
}
