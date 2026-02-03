// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::{HttpError, StateKeeper};
use axum::extract::State;
use axum::http::StatusCode;
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// GET | HEAD /ready
pub(super) async fn ready(
    State(keeper): State<Arc<StateKeeper>>,
    _http_error: HeaderMap,
) -> Result<StatusCode, HttpError> {
    match keeper.get_anonymous().await {
        Ok(components) => {
            components.storage.info().await?;
            Ok(StatusCode::OK)
        }
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{headers, keeper, waiting_keeper};
    use reduct_base::error::ErrorCode;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_alive(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let keeper = keeper.await;
        let res = ready(State(Arc::clone(&keeper)), headers).await.unwrap();
        assert_eq!(res, StatusCode::OK);
    }

    #[rstest]
    #[tokio::test]
    async fn test_alive_unavailable_for_legal_reasons(#[future] waiting_keeper: Arc<StateKeeper>) {
        let keeper = waiting_keeper.await;
        let err = ready(State(Arc::clone(&keeper)), HeaderMap::new())
            .await
            .err()
            .unwrap();
        assert_eq!(
            err.status(),
            ErrorCode::ServiceUnavailable,
            "Ready should return 503 if lock file isn't acquired"
        );
    }
}
