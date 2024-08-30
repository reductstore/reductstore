// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod common;
mod query;
mod read_batched;
mod read_single;
mod remove_entry;
mod remove_single;
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
use crate::storage::storage::Storage;
use axum::async_trait;
use axum::extract::FromRequest;

use axum_extra::headers::HeaderMapExt;

use crate::api::entry::remove_single::remove_record;
use crate::api::entry::update_batched::update_batched_records;
use crate::api::entry::update_single::update_record;
use axum::body::Body;
use axum::http::Request;
use axum::routing::{delete, get, head, patch, post};
use reduct_base::error::ErrorCode;
use reduct_base::msg::entry_api::QueryInfo;
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

async fn check_and_extract_ts_or_query_id(
    storage: &Storage,
    params: HashMap<String, String>,
    bucket_name: &String,
    entry_name: &String,
) -> Result<(Option<u64>, Option<u64>), HttpError> {
    let ts = match params.get("ts") {
        Some(ts) => Some(ts.parse::<u64>().map_err(|_| {
            HttpError::new(
                ErrorCode::UnprocessableEntity,
                "'ts' must be an unix timestamp in microseconds",
            )
        })?),
        None => None,
    };

    let query_id = match params.get("q") {
        Some(query) => Some(query.parse::<u64>().map_err(|_| {
            HttpError::new(ErrorCode::UnprocessableEntity, "'query' must be a number")
        })?),
        None => None,
    };

    let ts = if ts.is_none() && query_id.is_none() {
        Some(
            storage
                .get_bucket(bucket_name)?
                .get_entry(entry_name)?
                .info()
                .await?
                .latest_record,
        )
    } else {
        ts
    };
    Ok((query_id, ts))
}

#[derive(IntoResponse, Twin)]
pub struct QueryInfoAxum(QueryInfo);

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
        .route("/:bucket_name/:entry_name/q", get(query::query))
        .route("/:bucket_name/:entry_name", delete(remove_entry))
        .route("/:bucket_name/:entry_name", delete(remove_record))
}
