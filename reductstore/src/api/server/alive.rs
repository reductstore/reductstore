// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::{HttpError, StateKeeper};
use axum::extract::State;
use axum::http::StatusCode;
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// GET | HEAD /alive
pub(super) async fn alive(
    State(keeper): State<Arc<StateKeeper>>,
    _http_error: HeaderMap,
) -> Result<StatusCode, HttpError> {
    let components = keeper.get_anonymous().await?;
    components.storage.info()?;
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{headers, keeper};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_alive(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let keeper = keeper.await;
        let info = alive(State(Arc::clone(&keeper)), headers).await.unwrap();
        assert_eq!(info, StatusCode::OK);
    }
}
