// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::{Components, HttpError};
use axum::extract::State;
use axum::http::StatusCode;
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// GET | HEAD /alive
pub(crate) async fn alive(
    State(components): State<Arc<Components>>,
    _http_error: HeaderMap,
) -> Result<StatusCode, HttpError> {
    components.storage.info()?;
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{components, headers};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_alive(#[future] components: Arc<Components>, headers: HeaderMap) {
        let info = alive(State(components.await), headers).await.unwrap();
        assert_eq!(info, StatusCode::OK);
    }
}
