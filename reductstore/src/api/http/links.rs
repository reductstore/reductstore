// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::links::create::create;
use crate::api::http::{HttpError, StateKeeper};
use axum::extract::FromRequest;
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

pub(super) fn create_query_link_api_routes() -> axum::Router<Arc<StateKeeper>> {
    axum::Router::new()
        .route("/{*file_name}", post(create))
        .route("/{*file_name}", get(get::get))
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::api::http::links::create::create;
    use crate::api::http::links::{QueryLinkCreateRequestAxum, QueryLinkCreateResponseAxum};
    use crate::api::http::tests::{headers, keeper};
    use crate::api::http::HttpError;
    use axum::body::Body;
    use axum::extract::{Path, State};
    use axum::http::{HeaderMap, Method, Request, StatusCode};
    use chrono::{DateTime, Utc};
    use reduct_base::msg::entry_api::QueryEntry;
    use reduct_base::msg::query_link_api::QueryLinkCreateRequest;
    use rstest::rstest;
    use std::sync::Arc;
    use tower::ServiceExt;

    #[rstest]
    #[tokio::test]
    async fn test_from_request() {
        let http_request = axum::http::Request::builder()
            .header("content-type", "application/json")
            .body(axum::body::Body::from(
                r#"{
            "expire_at": 1000000000,
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
                expire_at: DateTime::<Utc>::from_timestamp_secs(1000000000).unwrap(),
                bucket: "bucket-1".to_string(),
                entry: "entry-1".to_string(),
                query: QueryEntry {
                    query_type: reduct_base::msg::entry_api::QueryType::Query,
                    ..Default::default()
                },
                ..Default::default()
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
            "expire_at": 1000000000,
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
        let err: ReductError = err.into();
        assert_eq!(err, unprocessable_entity!("Invalid JSON: unknown variant `INVALID_TYPE`, expected `QUERY` or `REMOVE` at line 6 column 44"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_route_with_multi_segment_file_name_post(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let app = create_query_link_api_routes().with_state(keeper.await);
        let auth = headers.get("authorization").unwrap().to_str().unwrap();
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/a/b/c")
                    .header("authorization", auth)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{
                            "expire_at": 4102444800,
                            "bucket": "bucket-1",
                            "entry": "entry-1",
                            "record_entry": "entry-1",
                            "record_timestamp": 0,
                            "query": {
                                "query_type": "QUERY"
                            }
                        }"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[rstest]
    #[tokio::test]
    async fn test_route_with_multi_segment_file_name_get(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let app = create_query_link_api_routes().with_state(keeper.clone());
        let link = create_query_link(
            headers,
            keeper,
            QueryEntry {
                query_type: reduct_base::msg::entry_api::QueryType::Query,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap()
        .0
        .link;
        let query = link.split('?').nth(1).unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(format!("/a/b/c?{}", query))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    pub(super) async fn create_query_link(
        headers: HeaderMap,
        keeper: Arc<StateKeeper>,
        query: QueryEntry,
        expire_at: Option<DateTime<Utc>>,
    ) -> Result<QueryLinkCreateResponseAxum, HttpError> {
        create(
            State(Arc::clone(&keeper)),
            headers,
            Path("file.txt".to_string()),
            QueryLinkCreateRequestAxum(QueryLinkCreateRequest {
                expire_at: expire_at.unwrap_or_else(|| Utc::now() + chrono::Duration::hours(1)),
                bucket: "bucket-1".to_string(),
                entry: "entry-1".to_string(),
                record_entry: Some("entry-1".to_string()),
                record_timestamp: Some(0),
                query,
                ..Default::default()
            }),
        )
        .await
    }
}
