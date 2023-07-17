// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::policy::WriteAccessPolicy;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::HttpServerState;
use crate::storage::entry::Labels;
use crate::storage::writer::{Chunk, RecordWriter};
use axum::extract::{BodyStream, Path, Query, State};
use axum::headers::{Expect, Header, HeaderMap};
use bytes::Bytes;
use futures_util::StreamExt;
use log::{debug, error};
use reduct_base::error::HttpError;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

// POST /:bucket/:entry?ts=<number>
pub async fn write_record(
    State(components): State<Arc<RwLock<HttpServerState>>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
    mut stream: BodyStream,
) -> Result<(), HttpError> {
    let bucket = path.get("bucket_name").unwrap();
    check_permissions(
        Arc::clone(&components),
        headers.clone(),
        WriteAccessPolicy {
            bucket: bucket.clone(),
        },
    )?;

    let check_request_and_get_writer = || -> Result<Arc<RwLock<RecordWriter>>, HttpError> {
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
                ));
            }
        };
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

        let mut labels = Labels::new();
        for (k, v) in headers.iter() {
            if k.as_str().starts_with("x-reduct-label-") {
                let key = k.as_str()[15..].to_string();
                let value = match v.to_str() {
                    Ok(value) => value.to_string(),
                    Err(_) => {
                        return Err(HttpError::unprocessable_entity(&format!(
                            "Label values for {} must be valid UTF-8 strings",
                            k
                        )));
                    }
                };
                labels.insert(key, value);
            }
        }

        let writer = {
            let mut components = components.write().unwrap();
            let bucket = components.storage.get_bucket(bucket)?;
            bucket.begin_write(
                path.get("entry_name").unwrap(),
                ts,
                content_size,
                content_type,
                labels,
            )?
        };
        Ok(writer)
    };

    match check_request_and_get_writer() {
        Ok(writer) => {
            while let Some(chunk) = stream.next().await {
                let mut writer = writer.write().unwrap();
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(e) => {
                        writer.write(Chunk::Error)?;
                        error!("Error while receiving data: {}", e);
                        return Err(HttpError::from(e));
                    }
                };
                writer.write(Chunk::Data(chunk))?;
            }

            writer.write().unwrap().write(Chunk::Last(Bytes::new()))?;
            Ok(())
        }
        Err(e) => {
            // drain the stream in the case if a reduct_client doesn't support Expect: 100-continue
            if !headers.contains_key(Expect::name()) {
                debug!("draining the stream");
                while let Some(_) = stream.next().await {}
            }

            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::body::Empty;
    use axum::extract::FromRequest;
    use axum::http::Request;

    use axum::headers::{Authorization, HeaderMapExt};
    use rstest::*;

    use crate::http_frontend::tests::{components, path_to_entry_1};

    #[rstest]
    #[tokio::test]
    async fn test_write_with_label_ok(
        components: Arc<RwLock<HttpServerState>>,
        headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
    ) {
        let emtpy_stream: Empty<Bytes> = Empty::new();
        let request = Request::builder().body(emtpy_stream).unwrap();
        let body = BodyStream::from_request(request, &()).await.unwrap();

        write_record(
            State(Arc::clone(&components)),
            headers,
            path_to_entry_1,
            Query(HashMap::from_iter(vec![(
                "ts".to_string(),
                "1".to_string(),
            )])),
            body,
        )
        .await
        .unwrap();

        let record = components
            .write()
            .unwrap()
            .storage
            .get_bucket("bucket-1")
            .unwrap()
            .begin_read("entry-1", 1)
            .unwrap();

        assert_eq!(
            record.read().unwrap().labels().get("x"),
            Some(&"y".to_string())
        );
    }

    #[fixture]
    pub fn headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("content-length", "0".parse().unwrap());
        headers.insert("x-reduct-label-x", "y".parse().unwrap());
        headers.typed_insert(Authorization::bearer("init-token").unwrap());

        headers
    }
}
