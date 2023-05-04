// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use axum::extract::{BodyStream, Path, Query, State};
use axum::http::header::HeaderMap;

use futures_util::stream::StreamExt;
use log::debug;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::core::status::HttpError;
use crate::http_frontend::HttpServerComponents;
use crate::storage::entry::Labels;

pub struct EntryApi {}

impl EntryApi {
    // POST /entries
    pub async fn write_record1(
        State(components): State<Arc<RwLock<HttpServerComponents>>>,
        headers: HeaderMap,
        Path(path): Path<HashMap<String, String>>,
        Query(params): Query<HashMap<String, String>>,
        mut stream: BodyStream,
    ) -> Result<(), HttpError> {
        if !params.contains_key("ts") {
            return Err(HttpError::unprocessable_entity(
                "'ts' parameter is required",
            ));
        }

        let ts = match params.get("ts").unwrap().parse::<u64>() {
            Ok(ts) => ts,
            Err(_) => {
                return Err(HttpError::unprocessable_entity(
                    "'ts' must be an unix timestamp in microseconds",
                ))
            }
        };
        debug!("headers: {:?}", headers);
        let content_size = headers
            .get("content-length")
            .ok_or(HttpError::unprocessable_entity(
                "content-length header is required",
            ))?
            .to_str()
            .unwrap()
            .parse::<u64>()
            .map_err(|_| {
                HttpError::unprocessable_entity("content-length header must be a number")
            })?;

        let content_type = headers
            .get("content-type")
            .map_or("application/octet-stream", |v| v.to_str().unwrap())
            .to_string();

        let labels = headers
            .iter()
            .filter(|(k, _)| k.as_str().starts_with("x-reduct-label-"))
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap().to_string()))
            .collect::<Labels>();

        let writer = {
            let mut components = components.write().unwrap();
            let bucket = components
                .storage
                .get_bucket(path.get("bucket_name").unwrap())?;
            bucket.begin_write(
                path.get("entry_name").unwrap(),
                ts,
                content_size,
                content_type,
                labels,
            )?
        };

        while let Some(chunk) = stream.next().await {
            let mut writer = writer.write().unwrap();
            let chunk = chunk.unwrap();
            writer.write(chunk.as_ref(), false).unwrap();
        }

        writer.write().unwrap().write(vec![].as_ref(), true)?;
        Ok(())
    }
}
