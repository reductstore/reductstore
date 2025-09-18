// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::links::create::create;
use crate::api::{Components, HttpError};
use axum::extract::FromRequest;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum_extra::headers::HeaderMapExt;
use bytes::Bytes;
use reduct_base::error::ReductError;
use reduct_base::msg::query_link_api::{QueryLinkCreateRequest, QueryLinkCreateResponse};
use reduct_base::unprocessable_entity;
use reduct_macros::{IntoResponse, Twin};
use std::sync::Arc;

mod create;
mod get;

#[derive(Twin)]
pub(super) struct QueryLinkCreateRequestAxum(QueryLinkCreateRequest);

#[derive(IntoResponse, Twin)]
pub(super) struct QueryLinkCreateResponseAxum(QueryLinkCreateResponse);

impl<S> FromRequest<S> for QueryLinkCreateRequestAxum
where
    Bytes: FromRequest<S>,
    S: Send + Sync,
{
    type Rejection = HttpError;

    async fn from_request(
        req: axum::http::Request<axum::body::Body>,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let bytes = Bytes::from_request(req, state)
            .await
            .map_err(|_| unprocessable_entity!("Invalid body"))?;
        let response = match serde_json::from_slice::<QueryLinkCreateRequest>(&*bytes) {
            Ok(x) => Ok(QueryLinkCreateRequestAxum::from(x)),
            Err(e) => Err(e.into()),
        };
        response
    }
}

pub(super) fn derive_key_from_secret(secret: &[u8], salt: &[u8]) -> [u8; 32] {
    use argon2::Argon2;
    let argon2 = Argon2::default();
    let mut key = [0u8; 32];
    argon2.hash_password_into(secret, salt, &mut key).unwrap();
    key
}

pub(super) fn create_query_link_api_routes() -> axum::Router<Arc<Components>> {
    axum::Router::new()
        .route("/", post(create))
        .route("/", get(get::get))
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::api::links::create::create;
    use crate::api::links::{QueryLinkCreateRequestAxum, QueryLinkCreateResponseAxum};
    use crate::api::{Components, HttpError};
    use axum::extract::State;
    use axum::http::HeaderMap;
    use chrono::{DateTime, Utc};
    use reduct_base::msg::entry_api::QueryEntry;
    use reduct_base::msg::query_link_api::QueryLinkCreateRequest;
    use rstest::rstest;
    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_from_request() {
        let http_request = axum::http::Request::builder()
            .header("content-type", "application/json")
            .body(axum::body::Body::from(
                r#"{
            "expire_at": null,
            "bucket": "bucket-1",
            "entry": "entry-1",
            "query": {
                "query_type": "QUERY"
            }
        }"#,
            ))
            .unwrap();

        let req = QueryLinkCreateRequestAxum::from_request(http_request, &State(()))
            .await
            .unwrap();

        assert_eq!(
            req.0,
            QueryLinkCreateRequest {
                expire_at: None,
                bucket: "bucket-1".to_string(),
                entry: "entry-1".to_string(),
                query: QueryEntry {
                    query_type: reduct_base::msg::entry_api::QueryType::Query,
                    ..Default::default()
                },
            }
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_from_request_invalid_body() {
        let http_request = axum::http::Request::builder()
            .header("content-type", "application/json")
            .body(axum::body::Body::from(
                r#"{
            "expire_at": null,
            "bucket": "bucket-1",
            "entry": "entry-1",
            "query": {
                "query_type": "INVALID_TYPE"
            }
        }"#,
            ))
            .unwrap();

        let err = QueryLinkCreateRequestAxum::from_request(http_request, &State(()))
            .await
            .err()
            .unwrap();
        assert_eq!(err.0, unprocessable_entity!("Invalid JSON: unknown variant `INVALID_TYPE`, expected `QUERY` or `REMOVE` at line 6 column 44"));
    }

    pub(super) async fn create_query_link(
        headers: HeaderMap,
        components: Arc<Components>,
        query: QueryEntry,
        expire_at: Option<DateTime<Utc>>,
    ) -> Result<QueryLinkCreateResponseAxum, HttpError> {
        create(
            State(Arc::clone(&components)),
            headers,
            QueryLinkCreateRequestAxum(QueryLinkCreateRequest {
                expire_at,
                bucket: "bucket-1".to_string(),
                entry: "entry-1".to_string(),
                query,
            }),
        )
        .await
    }
}
