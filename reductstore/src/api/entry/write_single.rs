// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::HttpError;
use crate::auth::policy::WriteAccessPolicy;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum_extra::headers::{Expect, Header, HeaderMap};

use crate::api::entry::common::{parse_content_length_from_header, parse_timestamp_from_query};
use crate::api::StateKeeper;
use crate::replication::Transaction::WriteRecord;
use crate::replication::TransactionNotification;
use futures_util::StreamExt;
use log::{debug, error};
use reduct_base::error::ReductError;
use reduct_base::io::RecordMeta;
use reduct_base::{bad_request, unprocessable_entity, Labels};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::timeout;

// POST /:bucket/:entry?ts=<number>
pub(super) async fn write_record(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
    body: Body,
) -> Result<(), HttpError> {
    let bucket = path.get("bucket_name").unwrap();
    let components = keeper
        .get_with_permissions(&headers.clone(), WriteAccessPolicy { bucket })
        .await?;

    let mut stream = body.into_data_stream();

    let check_request_and_get_sender = async {
        let ts = parse_timestamp_from_query(&params)?;
        let content_size = parse_content_length_from_header(&headers)?;
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
                        return Err(unprocessable_entity!(
                            "Label values for {} must be valid UTF-8 strings",
                            k
                        )
                        .into());
                    }
                };
                labels.insert(key, value);
            }
        }

        let sender = {
            let bucket = components.storage.get_bucket(bucket).await?.upgrade()?;
            bucket
                .begin_write(
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

    let io_timeout = components.cfg.io_conf.operation_timeout;
    match check_request_and_get_sender.await {
        Ok((ts, labels, mut writer)) => {
            macro_rules! send_chunk {
                ($chunk:expr) => {
                    writer.send_timeout($chunk, io_timeout).await?;
                };
            }

            while let Some(chunk) = timeout(io_timeout, stream.next())
                .await
                .map_err(|_| bad_request!("Timeout while receiving data"))?
            {
                let chunk = match chunk {
                    Ok(chunk) => Ok(Some(chunk)),
                    Err(e) => {
                        error!("Error while receiving data: {}", e);
                        let err = HttpError::from(e).into_inner();
                        send_chunk!(Err(err.clone()));
                        return Err(err.into());
                    }
                };

                send_chunk!(chunk);
            }

            if let Err(err) = writer.send_timeout(Ok(None), io_timeout).await {
                debug!("Timeout while sending EOF: {}", err);
            }

            components
                .replication_repo
                .write()
                .await?
                .notify(TransactionNotification {
                    bucket: bucket.clone(),
                    entry: path.get("entry_name").unwrap().to_string(),
                    meta: RecordMeta::builder().timestamp(ts).labels(labels).build(),
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

    use crate::api::tests::{empty_body, keeper, path_to_entry_1};

    use axum_extra::headers::{Authorization, HeaderMapExt};
    use reduct_base::io::ReadRecord;
    use reduct_base::not_found;
    use rstest::*;

    #[rstest]
    #[tokio::test]
    async fn test_write_with_label_ok(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] empty_body: Body,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        write_record(
            State(keeper),
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
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade_and_unwrap()
            .begin_read("entry-1", 1)
            .await
            .unwrap();

        assert_eq!(&record.meta().labels()["x"], "y");

        let info = components
            .replication_repo
            .read()
            .await
            .unwrap()
            .get_info("api-test")
            .await
            .unwrap();
        assert_eq!(info.info.pending_records, 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_bucket_not_found(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        #[future] empty_body: Body,
    ) {
        let path = Path(HashMap::from_iter(vec![
            ("bucket_name".to_string(), "XXX".to_string()),
            ("entry_name".to_string(), "entry-1".to_string()),
        ]));
        let err = write_record(
            State(keeper.await),
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

        assert_eq!(err, not_found!("Bucket 'XXX' is not found").into());
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_bad_ts(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] empty_body: Body,
    ) {
        let err = write_record(
            State(keeper.await),
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
            unprocessable_entity!("'ts' must be an unix timestamp in microseconds").into()
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
