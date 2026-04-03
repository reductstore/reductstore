// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::token::{populate_token_status, TokenListAxum};
use crate::api::http::{HttpError, StateKeeper};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::State;
use axum::http::HeaderMap;

use std::sync::Arc;

// GET /tokens
pub(super) async fn list_tokens(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
) -> Result<TokenListAxum, HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;
    let token_list = components
        .token_repo
        .write()
        .await?
        .get_token_list_with_last_access()
        .await?;

    let mut list = TokenListAxum::default();
    for token in token_list {
        let mut x = token;
        x.value.clear();
        populate_token_status(&mut x);
        list.0.tokens.push(x);
    }
    list.0.tokens.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(list)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::api::http::tests::{headers, keeper};
    use crate::audit::AUDIT_BUCKET_NAME;
    use bytes::Bytes;
    use reduct_base::msg::bucket_api::BucketSettings;
    use reduct_base::Labels;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_token_list(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let list = list_tokens(State(keeper.await), headers).await.unwrap().0;
        assert_eq!(list.tokens.len(), 2);
        assert_eq!(list.tokens[0].name, "init-token");
        assert_eq!(list.tokens[0].last_access, None);
        assert!(list.tokens[0].value.is_empty(), "Token value MUST be empty");
        assert_eq!(list.tokens[1].name, "test");
        assert_eq!(list.tokens[1].last_access, None);
        assert!(list.tokens[1].value.is_empty(), "Token value MUST be empty");
    }

    #[rstest]
    #[tokio::test]
    async fn test_token_list_with_last_access(
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
                2_000_000,
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

        let list = list_tokens(State(keeper), headers).await.unwrap().0;
        let token = list
            .tokens
            .iter()
            .find(|token| token.name == "test")
            .unwrap();
        assert_eq!(
            token.last_access,
            chrono::DateTime::from_timestamp_micros(2_000_000)
        );
    }
}
