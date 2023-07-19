// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::policy::ReadAccessPolicy;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::{HttpError, HttpServerState};
use crate::storage::proto::QueryInfo;
use crate::storage::query::base::QueryOptions;

use axum::extract::{Path, Query, State};
use axum::headers::HeaderMap;

use std::collections::HashMap;

use std::sync::Arc;

use reduct_base::error::HttpStatus;
use std::time::Duration;

// GET /:bucket/:entry/q?start=<number>&stop=<number>&continue=<number>&exclude-<label>=<value>&include-<label>=<value>&ttl=<number>
pub async fn query(
    State(components): State<Arc<HttpServerState>>,
    Path(path): Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<QueryInfo, HttpError> {
    let bucket_name = path.get("bucket_name").unwrap();
    let entry_name = path.get("entry_name").unwrap();

    check_permissions(
        &components,
        headers,
        ReadAccessPolicy {
            bucket: bucket_name.clone(),
        },
    )
    .await?;

    let entry_info = {
        let mut storage = components.storage.write().await;
        let bucket = storage.get_mut_bucket(bucket_name)?;
        bucket.get_entry(entry_name)?.info()?
    };

    let start = match params.get("start") {
        Some(start) => start.parse::<u64>().map_err(|_| {
            HttpError::new(
                HttpStatus::UnprocessableEntity,
                "'start' must be an unix timestamp in microseconds",
            )
        })?,
        None => entry_info.oldest_record,
    };

    let stop = match params.get("stop") {
        Some(stop) => stop.parse::<u64>().map_err(|_| {
            HttpError::new(
                HttpStatus::UnprocessableEntity,
                "'stop' must be an unix timestamp in microseconds",
            )
        })?,
        None => entry_info.latest_record + 1,
    };

    let continuous = match params.get("continuous") {
        Some(continue_) => continue_.parse::<bool>().map_err(|_| {
            HttpError::new(
                HttpStatus::UnprocessableEntity,
                "'continue' must be an unix timestamp in microseconds",
            )
        })?,
        None => false,
    };

    let ttl = match params.get("ttl") {
        Some(ttl) => ttl.parse::<u64>().map_err(|_| {
            HttpError::new(
                HttpStatus::UnprocessableEntity,
                "'ttl' must be an unix timestamp in microseconds",
            )
        })?,
        None => 5,
    };

    let mut include = HashMap::new();
    let mut exclude = HashMap::new();

    for (k, v) in params.iter() {
        if k.starts_with("include-") {
            include.insert(k[8..].to_string(), v.to_string());
        } else if k.starts_with("exclude-") {
            exclude.insert(k[8..].to_string(), v.to_string());
        }
    }

    let mut storage = components.storage.write().await;
    let bucket = storage.get_mut_bucket(bucket_name)?;
    let entry = bucket.get_or_create_entry(entry_name)?;
    let id = entry.query(
        start,
        stop,
        QueryOptions {
            continuous,
            include,
            exclude,
            ttl: Duration::from_secs(ttl),
        },
    )?;

    Ok(QueryInfo { id })
}
