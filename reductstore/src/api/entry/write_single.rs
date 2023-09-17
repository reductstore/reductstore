// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::{Componentes, ErrorCode, HttpError};
use crate::auth::policy::WriteAccessPolicy;
use crate::storage::entry::Labels;
use crate::storage::writer::Chunk;
use axum::extract::{BodyStream, Path, Query, State};
use axum::headers::{Expect, Header, HeaderMap};
use bytes::Bytes;
use futures_util::StreamExt;
use log::{debug, error};
use std::collections::HashMap;
use std::sync::Arc;

// POST /:bucket/:entry?ts=<number>
pub async fn write_record(
    State(components): State<Arc<Componentes>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
    mut stream: BodyStream,
) -> Result<(), HttpError> {
    let bucket = path.get("bucket_name").unwrap();
    check_permissions(
        &components,
        headers.clone(),
        WriteAccessPolicy {
            bucket: bucket.clone(),
        },
    )
    .await?;

    let check_request_and_get_writer = async {
        if !params.contains_key("ts") {
            return Err(HttpError::new(
                ErrorCode::UnprocessableEntity,
                "'ts' parameter is required",
            ));
        }

        let ts = match params.get("ts").unwrap().parse::<u64>() {
            Ok(ts) => ts,
            Err(_) => {
                return Err(HttpError::new(
                    ErrorCode::UnprocessableEntity,
                    "'ts' must be an unix timestamp in microseconds",
                ));
            }
        };
        let content_size = headers
            .get("content-length")
            .ok_or(HttpError::new(
                ErrorCode::UnprocessableEntity,
                "content-length header is required",
            ))?
            .to_str()
            .unwrap()
            .parse::<usize>()
            .map_err(|_| {
                HttpError::new(
                    ErrorCode::UnprocessableEntity,
                    "content-length header must be a number",
                )
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
                        return Err(HttpError::new(
                            ErrorCode::UnprocessableEntity,
                            &format!("Label values for {} must be valid UTF-8 strings", k),
                        ));
                    }
                };
                labels.insert(key, value);
            }
        }

        let writer = {
            let mut storage = components.storage.write().await;
            let bucket = storage.get_mut_bucket(bucket)?;
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

    match check_request_and_get_writer.await {
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
            // drain the stream in the case if a client doesn't support Expect: 100-continue
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

    use crate::api::tests::{components, empty_body, path_to_entry_1};

    #[rstest]
    #[tokio::test]
    async fn test_write_with_label_ok(
        components: Arc<Componentes>,
        headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] empty_body: BodyStream,
    ) {
        write_record(
            State(Arc::clone(&components)),
            headers,
            path_to_entry_1,
            Query(HashMap::from_iter(vec![(
                "ts".to_string(),
                "1".to_string(),
            )])),
            empty_body.await,
        )
        .await
        .unwrap();

        let record = components
            .storage
            .read()
            .await
            .get_bucket("bucket-1")
            .unwrap()
            .begin_read("entry-1", 1)
            .unwrap();

        assert_eq!(
            record.read().unwrap().labels().get("x"),
            Some(&"y".to_string())
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_bucket_not_found(
        components: Arc<Componentes>,
        headers: HeaderMap,
        #[future] empty_body: BodyStream,
    ) {
        let path = Path(HashMap::from_iter(vec![
            ("bucket_name".to_string(), "XXX".to_string()),
            ("entry_name".to_string(), "entry-1".to_string()),
        ]));
        let err = write_record(
            State(Arc::clone(&components)),
            headers,
            path,
            Query(HashMap::from_iter(vec![(
                "ts".to_string(),
                "1".to_string(),
            )])),
            empty_body.await,
        )
        .await
        .err()
        .unwrap();

        assert_eq!(
            err,
            HttpError::new(ErrorCode::NotFound, "Bucket 'XXX' is not found")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_bad_ts(
        components: Arc<Componentes>,
        headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] empty_body: BodyStream,
    ) {
        let err = write_record(
            State(Arc::clone(&components)),
            headers,
            path_to_entry_1,
            Query(HashMap::from_iter(vec![(
                "ts".to_string(),
                "bad".to_string(),
            )])),
            empty_body.await,
        )
        .await
        .err()
        .unwrap();

        assert_eq!(
            err,
            HttpError::new(
                ErrorCode::UnprocessableEntity,
                "'ts' must be an unix timestamp in microseconds",
            )
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
