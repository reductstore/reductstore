// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod query;
mod read_batched;
mod read_single;
mod write;

use axum::async_trait;
use axum::body::StreamBody;
use axum::extract::{BodyStream, FromRequest, Path, Query, State};
use axum::http::header::HeaderMap;
use axum::http::{HeaderName, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures_util::stream::StreamExt;
use futures_util::Stream;

use std::collections::HashMap;

use crate::auth::policy::{ReadAccessPolicy, WriteAccessPolicy};
use axum::headers;
use axum::headers::{Expect, Header, HeaderMapExt, HeaderValue};

use axum::routing::{get, head, post};
use log::{debug, error};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use crate::core::status::{HttpError, HttpStatus};
use crate::http_frontend::entry_api::read_batched::read_batched_records;
use crate::http_frontend::entry_api::read_single::read_single_record;
use crate::http_frontend::entry_api::write::write_record;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::HttpServerState;
use crate::storage::bucket::Bucket;
use crate::storage::entry::Labels;
use crate::storage::proto::QueryInfo;
use crate::storage::query::base::QueryOptions;
use crate::storage::reader::RecordReader;
use crate::storage::writer::{Chunk, RecordWriter};

pub struct EntryApi {}

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
    components: &Arc<RwLock<HttpServerState>>,
    params: HashMap<String, String>,
    bucket_name: &String,
    entry_name: &String,
) -> Result<(Option<u64>, Option<u64>), HttpError> {
    let ts = match params.get("ts") {
        Some(ts) => Some(ts.parse::<u64>().map_err(|_| {
            HttpError::unprocessable_entity("'ts' must be an unix timestamp in microseconds")
        })?),
        None => None,
    };

    let query_id = match params.get("q") {
        Some(query) => Some(
            query
                .parse::<u64>()
                .map_err(|_| HttpError::unprocessable_entity("'query' must be a number"))?,
        ),
        None => None,
    };

    let ts = if ts.is_none() && query_id.is_none() {
        let mut components = components.write().unwrap();
        Some(
            components
                .storage
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

impl IntoResponse for QueryInfo {
    fn into_response(self) -> Response {
        let mut headers = HeaderMap::new();
        headers.typed_insert(headers::ContentType::json());

        (
            StatusCode::OK,
            headers,
            serde_json::to_string(&self).unwrap(),
        )
            .into_response()
    }
}

impl EntryApi {}

pub fn create_entry_api_routes() -> axum::Router<Arc<RwLock<HttpServerState>>> {
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

    use rstest::*;
    use std::path::PathBuf;

    #[fixture]
    pub fn components() -> Arc<RwLock<HttpServerState>> {
        let data_path = tempfile::tempdir().unwrap().into_path();

        let mut components = HttpServerState {
            storage: Storage::new(PathBuf::from(data_path.clone())),
            auth: TokenAuthorization::new(""),
            token_repo: create_token_repository(data_path.clone(), ""),
            console: ZipAssetManager::new(&[]),
            base_path: "/".to_string(),
        };

        let labels = HashMap::from_iter(vec![
            ("x".to_string(), "y".to_string()),
            ("b".to_string(), "[a,b]".to_string()),
        ]);
        components
            .storage
            .create_bucket("bucket-1", BucketSettings::default())
            .unwrap()
            .begin_write("entry-1", 0, 6, "text/plain".to_string(), labels)
            .unwrap()
            .write()
            .unwrap()
            .write(Chunk::Last(Bytes::from("Hey!!!")))
            .unwrap();
        Arc::new(RwLock::new(components))
    }

    #[fixture]
    pub fn headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("content-length", "0".parse().unwrap());
        headers.insert("x-reduct-label-x", "y".parse().unwrap());
        headers
    }

    #[fixture]
    pub fn path() -> Path<HashMap<String, String>> {
        let path = Path(HashMap::from_iter(vec![
            ("bucket_name".to_string(), "bucket-1".to_string()),
            ("entry_name".to_string(), "entry-1".to_string()),
        ]));
        path
    }
}
