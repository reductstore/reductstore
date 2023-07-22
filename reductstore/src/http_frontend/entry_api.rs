// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod query;
mod read_batched;
mod read_single;
mod write;

use axum::async_trait;

use axum::extract::FromRequest;
use axum::http::header::HeaderMap;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};

use std::collections::HashMap;

use axum::headers;
use axum::headers::HeaderMapExt;

use axum::routing::{get, head, post};

use crate::storage::storage::Storage;
use reduct_base::error::ErrorCode;
use reduct_base::msg::entry_api::QueryInfo;
use reduct_macros::{IntoResponse, Twin};
use std::sync::Arc;

use crate::http_frontend::entry_api::read_batched::read_batched_records;
use crate::http_frontend::entry_api::read_single::read_single_record;
use crate::http_frontend::entry_api::write::write_record;
use crate::http_frontend::HttpError;

use crate::http_frontend::HttpServerState;

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
impl<S, B> FromRequest<S, B> for MethodExtractor
where
    S: Send + Sync + 'static,
    B: Send + Sync + 'static,
{
    type Rejection = HttpError;

    async fn from_request(req: Request<B>, _: &S) -> Result<Self, Self::Rejection> {
        let method = req.method().to_string();
        Ok(MethodExtractor { name: method })
    }
}

fn check_and_extract_ts_or_query_id(
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
                .info()?
                .latest_record,
        )
    } else {
        ts
    };
    Ok((query_id, ts))
}

#[derive(IntoResponse, Twin)]
pub struct QueryInfoAxum(QueryInfo);

pub fn create_entry_api_routes() -> axum::Router<Arc<HttpServerState>> {
    axum::Router::new()
        .route("/:bucket_name/:entry_name", post(write_record))
        .route("/:bucket_name/:entry_name", get(read_single_record))
        .route("/:bucket_name/:entry_name", head(read_single_record))
        .route("/:bucket_name/:entry_name/batch", get(read_batched_records))
        .route(
            "/:bucket_name/:entry_name/batch",
            head(read_batched_records),
        )
        .route("/:bucket_name/:entry_name/q", get(query::query))
}
