// Copyright 2023-2024 ReductStore
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::{Components, ErrorCode, HttpError};
use crate::auth::policy::WriteAccessPolicy;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum_extra::headers::{Expect, Header, HeaderMap};

use crate::replication::Transaction::WriteRecord;
use crate::replication::TransactionNotification;
use crate::storage::proto::record::Label;
use futures_util::StreamExt;
use log::{debug, error};
use reduct_base::error::ReductError;
use reduct_base::Labels;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

// POST /:bucket/:entry?ts=<number>
pub(crate) async fn write_record(
    State(components): State<Arc<Components>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
    body: Body,
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

    let mut stream = body.into_data_stream();

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
                    labels.clone(),
                )
                .await?
        };
        Ok((ts, labels, sender))
    };

    match check_request_and_get_sender.await {
        Ok((ts, labels, tx)) => {
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
            components
                .replication_repo
                .write()
                .await
                .notify(TransactionNotification {
                    bucket: bucket.clone(),
                    entry: path.get("entry_name").unwrap().to_string(),
                    labels: labels
                        .into_iter()
                        .map(|(k, v)| Label { name: k, value: v })
                        .collect(),
                    event: WriteRecord(ts),
                })
                .await?;
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

    use crate::api::tests::{components, empty_body, path_to_entry_1};
    use crate::storage::proto::record::Label;
    use axum_extra::headers::{Authorization, HeaderMapExt};
    use rstest::*;

    #[rstest]
    #[tokio::test]
    async fn test_write_with_label_ok(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] empty_body: Body,
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
        #[future] empty_body: Body,
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
        #[future] empty_body: Body,
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
