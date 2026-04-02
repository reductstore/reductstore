// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::token::TokenAxum;
use crate::api::http::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use std::sync::Arc;

// GET /tokens/:token_name
pub(super) async fn get_token(
    State(keeper): State<Arc<StateKeeper>>,
    Path(token_name): Path<String>,
    headers: HeaderMap,
) -> Result<TokenAxum, HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;

    let mut token = components
        .token_repo
        .write()
        .await?
        .get_token_with_last_access(&token_name)
        .await?;
    token.value.clear();
    Ok(TokenAxum::from(token))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::api::http::tests::{headers, keeper};
    use crate::audit::AUDIT_BUCKET_NAME;
    use bytes::Bytes;
    use reduct_base::msg::bucket_api::BucketSettings;
    use reduct_base::Labels;

    use reduct_base::error::ErrorCode;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_get_token(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let token = get_token(State(keeper.await), Path("test".to_string()), headers)
            .await
            .unwrap()
            .0;
        assert_eq!(token.name, "test");
        assert_eq!(token.last_access, None);
        assert!(token.value.is_empty(), "Token value MUST be empty")
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_token_not_found(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let err = get_token(State(keeper.await), Path("not-found".to_string()), headers)
            .await
            .err()
            .unwrap();
        assert_eq!(
            err,
            HttpError::new(ErrorCode::NotFound, "Token 'not-found' doesn't exist")
        )
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_token_with_last_access(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        components
            .storage
            .create_system_bucket(AUDIT_BUCKET_NAME, BucketSettings::default())
            .await
            .unwrap();

        let bucket = components
            .storage
            .get_bucket(AUDIT_BUCKET_NAME)
            .await
            .unwrap()
            .upgrade_and_unwrap();
        let mut writer = bucket
            .begin_write(
                "test",
                1_000_000,
                2,
                "application/json".to_string(),
                Labels::new(),
            )
            .await
            .unwrap();
        writer
            .send(Ok(Some(Bytes::from_static(b"{}"))))
            .await
            .unwrap();
        writer.send(Ok(None)).await.unwrap();

        let token = get_token(State(keeper), Path("test".to_string()), headers)
            .await
            .unwrap()
            .0;
        assert_eq!(
            token.last_access,
            chrono::DateTime::from_timestamp_micros(1_000_000)
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_token_uses_cached_last_access(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        components
            .storage
            .create_system_bucket(AUDIT_BUCKET_NAME, BucketSettings::default())
            .await
            .unwrap();

        let bucket = components
            .storage
            .get_bucket(AUDIT_BUCKET_NAME)
            .await
            .unwrap()
            .upgrade_and_unwrap();
        let mut first_writer = bucket
            .begin_write(
                "test",
                3_000_000,
                2,
                "application/json".to_string(),
                Labels::new(),
            )
            .await
            .unwrap();
        first_writer
            .send(Ok(Some(Bytes::from_static(b"{}"))))
            .await
            .unwrap();
        first_writer.send(Ok(None)).await.unwrap();

        let first = get_token(
            State(keeper.clone()),
            Path("test".to_string()),
            headers.clone(),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(
            first.last_access,
            chrono::DateTime::from_timestamp_micros(3_000_000)
        );

        let mut second_writer = bucket
            .begin_write(
                "test",
                4_000_000,
                2,
                "application/json".to_string(),
                Labels::new(),
            )
            .await
            .unwrap();
        second_writer
            .send(Ok(Some(Bytes::from_static(b"{}"))))
            .await
            .unwrap();
        second_writer.send(Ok(None)).await.unwrap();

        let second = get_token(State(keeper), Path("test".to_string()), headers)
            .await
            .unwrap()
            .0;
        assert_eq!(
            second.last_access,
            chrono::DateTime::from_timestamp_micros(3_000_000)
        );
    }
}
