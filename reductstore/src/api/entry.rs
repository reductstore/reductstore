// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod common;
mod read_batched;
mod read_query;
mod read_query_post;
mod read_single;
mod remove_batched;
mod remove_entry;
mod remove_query;
mod remove_query_post;
mod remove_single;
mod rename_entry;
mod update_batched;
mod update_single;
mod write_batched;
mod write_single;

use crate::api::entry::read_batched::read_batched_records;
use crate::api::entry::read_single::read_record;
use crate::api::entry::remove_entry::remove_entry;

use crate::api::entry::write_batched::write_batched_records;
use crate::api::entry::write_single::write_record;
use crate::api::Components;
use crate::api::HttpError;
use axum::async_trait;
use axum::extract::{FromRequest, Path, Query, State};

use axum_extra::headers::HeaderMapExt;

use crate::api::entry::remove_batched::remove_batched_records;
use crate::api::entry::remove_query::remove_query;
use crate::api::entry::remove_single::remove_record;
use crate::api::entry::rename_entry::rename_entry;
use crate::api::entry::update_batched::update_batched_records;
use crate::api::entry::update_single::update_record;

use crate::api::bucket::BucketSettingsAxum;
use crate::api::entry::read_query_post::read_query_json;
use crate::api::entry::remove_query_post::remove_query_json;
use axum::body::Body;
use axum::http::{HeaderMap, Request};
use axum::response::IntoResponse;
use axum::routing::{delete, get, head, patch, post, put};
use axum_macros::debug_handler;
use bytes::Bytes;
use hyper::Response;
use reduct_base::msg::bucket_api::BucketSettings;
use reduct_base::msg::entry_api::{QueryEntry, QueryInfo, QueryType, RemoveQueryInfo};
use reduct_macros::{IntoResponse, Twin};
use std::collections::HashMap;
use std::sync::Arc;

pub struct MethodExtractor {
    name: String,
}

impl MethodExtractor {
    pub fn name(&self) -> &str {
        &self.name
    }

    #[cfg(test)]
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

#[async_trait]
impl<S> FromRequest<S> for MethodExtractor
where
    S: Send + Sync + 'static,
{
    type Rejection = HttpError;

    async fn from_request(req: Request<Body>, _: &S) -> Result<Self, Self::Rejection> {
        let method = req.method().to_string();
        Ok(MethodExtractor { name: method })
    }
}

#[derive(IntoResponse, Twin)]
pub struct QueryInfoAxum(QueryInfo);

#[derive(IntoResponse, Twin)]
pub struct RemoveQueryInfoAxum(RemoveQueryInfo);

#[derive(IntoResponse, Twin)]
pub struct QueryEntryAxum(QueryEntry);

#[async_trait]
impl<S> FromRequest<S> for QueryEntryAxum
where
    Bytes: FromRequest<S>,
    S: Send + Sync,
{
    type Rejection = Response<Body>;

    async fn from_request(req: Request<Body>, state: &S) -> Result<Self, Self::Rejection> {
        let body = Bytes::from_request(req, state)
            .await
            .map_err(IntoResponse::into_response)?;

        let query: QueryEntry =
            serde_json::from_slice(&body).map_err(|e| HttpError::from(e).into_response())?;
        Ok(QueryEntryAxum(query))
    }
}

// Workaround for DELETE /:bucket/:entry and DELETE /:bucket/:entry?ts=<number>
async fn remove_entry_router(
    components: State<Arc<Components>>,
    headers: HeaderMap,
    path: Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<(), HttpError> {
    if params.is_empty() {
        remove_entry(components, path, headers).await
    } else {
        remove_record(components, headers, path, Query(params)).await
    }
}

#[debug_handler]
async fn query_entry_router(
    components: State<Arc<Components>>,
    headers: HeaderMap,
    path: Path<HashMap<String, String>>,
    request: QueryEntryAxum,
) -> Response<Body> {
    let request = request.0;
    match request.query_type {
        QueryType::Query => read_query_json(components, path, request, headers)
            .await
            .into_response(),
        QueryType::Remove => remove_query_json(components, path, request, headers)
            .await
            .into_response(),
    }
}

pub(crate) fn create_entry_api_routes() -> axum::Router<Arc<Components>> {
    axum::Router::new()
        .route("/:bucket_name/:entry_name", post(write_record))
        .route(
            "/:bucket_name/:entry_name/batch",
            post(write_batched_records),
        )
        .route("/:bucket_name/:entry_name", patch(update_record))
        .route(
            "/:bucket_name/:entry_name/batch",
            patch(update_batched_records),
        )
        .route("/:bucket_name/:entry_name", get(read_record))
        .route("/:bucket_name/:entry_name", head(read_record))
        .route("/:bucket_name/:entry_name/batch", get(read_batched_records))
        .route(
            "/:bucket_name/:entry_name/batch",
            head(read_batched_records),
        )
        .route("/:bucket_name/:entry_name/q", get(read_query::read_query)) // deprecated
        .route("/:bucket_name/:entry_name", delete(remove_entry_router))
        .route(
            "/:bucket_name/:entry_name/batch",
            delete(remove_batched_records),
        )
        .route("/:bucket_name/:entry_name/q", delete(remove_query)) // deprecated
        .route("/:bucket_name/:entry_name/rename", put(rename_entry))
        .route("/:bucket_name/:entry_name/q", post(query_entry_router))
}

#[cfg(test)]
mod tests {
    use super::*;

    use reduct_base::msg::entry_api::QueryEntry;
    pub async fn query(
        path_to_entry_1: &Path<HashMap<String, String>>,
        components: Arc<Components>,
    ) -> u64 {
        let options = QueryEntry {
            start: Some(0),
            stop: Some(u64::MAX),
            ..Default::default()
        };

        let query_id = {
            components
                .storage
                .get_bucket(path_to_entry_1.get("bucket_name").unwrap())
                .unwrap()
                .upgrade()
                .unwrap()
                .get_entry(path_to_entry_1.get("entry_name").unwrap())
                .unwrap()
                .upgrade()
                .unwrap()
                .query(options)
                .await
                .unwrap()
        };
        query_id
    }
}
