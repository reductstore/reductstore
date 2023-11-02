// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::{Components, ErrorCode, HttpError};
use crate::auth::policy::WriteAccessPolicy;
use crate::storage::entry::Labels;
use axum::extract::{BodyStream, Path, Query, State};
use axum::headers::{Expect, Header, HeaderMap};

use futures_util::StreamExt;
use log::{debug, error};
use reduct_base::error::ReductError;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

// POST /:bucket/:entry?ts=<number>
pub async fn write_record(
    State(components): State<Arc<Components>>,
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

    let check_request_and_get_sender = async {
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

        let sender = {
            let mut storage = components.storage.write().await;
            let bucket = storage.get_mut_bucket(bucket)?;
            bucket
                .write_record(
                    path.get("entry_name").unwrap(),
                    ts,
                    content_size,
                    content_type,
                    labels,
                )
                .await?
        };
        Ok(sender)
    };

    match check_request_and_get_sender.await {
        Ok(tx) => {
            macro_rules! send_chunk {
                ($chunk:expr) => {
                    tx.send($chunk).await.map(|_| ()).map_err(|e| {
                        error!("Error while writing data: {}", e);
                        HttpError::from(ReductError::bad_request(&format!(
                            "Error while writing data: {}",
                            e
                        )))
                    })?;
                };
            }

            while let Some(chunk) = stream.next().await {
                let chunk = match chunk {
                    Ok(chunk) => Ok(Some(chunk)),
                    Err(e) => {
                        error!("Error while receiving data: {}", e);
                        let err = HttpError::from(e).0;
                        send_chunk!(Err(err.clone()));
                        return Err(err.into());
                    }
                };

                send_chunk!(chunk);
            }

            if let Err(_) = tx.send_timeout(Ok(None), Duration::from_millis(1)).await {
                debug!("Failed to sync the channel - it may be already closed");
            }
            tx.closed().await; //sync with the storage

            Ok(())
        }
        Err(e) => {
            // drain the stream in the case if a client doesn't support Expect: 100-continue
            if !!headers.contains_key(Expect::name()) {
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

    use axum::headers::{Authorization, HeaderMapExt};
    use rstest::*;
    use tokio::time::sleep;

    use crate::api::tests::{components, empty_body, path_to_entry_1};
    use crate::storage::proto::record::Label;

    #[rstest]
    #[tokio::test]
    async fn test_write_with_label_ok(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] empty_body: BodyStream,
    ) {
        let components = components.await;
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

        sleep(std::time::Duration::from_millis(10)).await; // wait for the record to be written

        let record = components
            .storage
            .read()
            .await
            .get_bucket("bucket-1")
            .unwrap()
            .begin_read("entry-1", 1)
            .await
            .unwrap();

        assert_eq!(
            record.labels()[0],
            Label {
                name: "x".to_string(),
                value: "y".to_string(),
            }
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_bucket_not_found(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
        #[future] empty_body: BodyStream,
    ) {
        let components = components.await;
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
        #[future] components: Arc<Components>,
        headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] empty_body: BodyStream,
    ) {
        let components = components.await;
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
