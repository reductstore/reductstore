// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::auth::policy::AuthenticatedPolicy;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::server_api::BucketInfoListAxum;
use crate::http_frontend::{HttpError, HttpServerState};
use axum::extract::State;
use axum::headers::HeaderMap;
use std::sync::Arc;

// GET /list
pub async fn list(
    State(components): State<Arc<HttpServerState>>,
    headers: HeaderMap,
) -> Result<BucketInfoListAxum, HttpError> {
    check_permissions(&components, headers, AuthenticatedPolicy {}).await?;

    let list = components.storage.read().await.get_bucket_list()?;
    Ok(BucketInfoListAxum::from(list))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_frontend::tests::{components, headers};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_list(components: Arc<HttpServerState>, headers: HeaderMap) {
        let list = list(State(components), headers).await.unwrap();
        assert_eq!(list.0.buckets.len(), 2);
    }
}
