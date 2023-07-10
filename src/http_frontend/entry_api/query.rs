// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::policy::{ReadAccessPolicy, WriteAccessPolicy};
use crate::core::status::HttpError;
use crate::http_frontend::entry_api::{check_and_extract_ts_or_query_id, MethodExtractor};
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::HttpServerState;
use crate::storage::bucket::Bucket;
use crate::storage::entry::Labels;
use crate::storage::proto::QueryInfo;
use crate::storage::query::base::QueryOptions;
use crate::storage::reader::RecordReader;
use crate::storage::writer::{Chunk, RecordWriter};
use axum::body::StreamBody;
use axum::extract::{BodyStream, Path, Query, State};
use axum::headers::{Expect, Header, HeaderMap, HeaderName};
use axum::response::IntoResponse;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use log::{debug, error};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

// GET /:bucket/:entry/q?start=<number>&stop=<number>&continue=<number>&exclude-<label>=<value>&include-<label>=<value>&ttl=<number>
pub async fn query(
    State(components): State<Arc<RwLock<HttpServerState>>>,
    Path(path): Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<QueryInfo, HttpError> {
    let bucket_name = path.get("bucket_name").unwrap();
    let entry_name = path.get("entry_name").unwrap();

    check_permissions(
        Arc::clone(&components),
        headers,
        ReadAccessPolicy {
            bucket: bucket_name.clone(),
        },
    )?;

    let entry_info = {
        let mut components = components.write().unwrap();
        let bucket = components.storage.get_bucket(bucket_name)?;
        bucket.get_entry(entry_name)?.info()?
    };

    let start = match params.get("start") {
        Some(start) => start.parse::<u64>().map_err(|_| {
            HttpError::unprocessable_entity("'start' must be an unix timestamp in microseconds")
        })?,
        None => entry_info.oldest_record,
    };

    let stop = match params.get("stop") {
        Some(stop) => stop.parse::<u64>().map_err(|_| {
            HttpError::unprocessable_entity("'stop' must be an unix timestamp in microseconds")
        })?,
        None => entry_info.latest_record + 1,
    };

    let continuous = match params.get("continuous") {
        Some(continue_) => continue_.parse::<bool>().map_err(|_| {
            HttpError::unprocessable_entity("'continue' must be an unix timestamp in microseconds")
        })?,
        None => false,
    };

    let ttl = match params.get("ttl") {
        Some(ttl) => ttl.parse::<u64>().map_err(|_| {
            HttpError::unprocessable_entity("'ttl' must be an unix timestamp in microseconds")
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

    let mut components = components.write().unwrap();
    let bucket = components.storage.get_bucket(bucket_name)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::asset_manager::ZipAssetManager;
    use crate::auth::token_auth::TokenAuthorization;
    use crate::auth::token_repository::create_token_repository;
    use crate::storage::proto::BucketSettings;
    use crate::storage::storage::Storage;
    use axum::body::{Empty, HttpBody};
    use axum::extract::FromRequest;
    use axum::http::Request;

    use crate::http_frontend::entry_api::tests::components;
    use rstest::*;
    use std::path::PathBuf;

    fn query_records(components: &Arc<RwLock<HttpServerState>>) -> u64 {
        let query_id = components
            .write()
            .unwrap()
            .storage
            .get_bucket("bucket-1")
            .unwrap()
            .get_entry("entry-1")
            .unwrap()
            .query(
                0,
                1,
                QueryOptions {
                    continuous: false,
                    include: HashMap::new(),
                    exclude: HashMap::new(),
                    ttl: Duration::from_secs(1),
                },
            )
            .unwrap();
        query_id
    }
}
