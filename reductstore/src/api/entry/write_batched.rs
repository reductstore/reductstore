// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::{Components, ErrorCode, HttpError};
use crate::auth::policy::WriteAccessPolicy;
use axum::extract::{BodyStream, Path, State};
use axum::headers::{Expect, Header, HeaderMap, HeaderValue};
use axum::http::HeaderName;
use axum::response::IntoResponse;
use bytes::Bytes;
use futures_util::StreamExt;

use log::debug;
use reduct_base::batch::{parse_batched_header, sort_headers_by_name, RecordHeader};
use reduct_base::error::ReductError;
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

// POST /:bucket/:entry/batch
pub async fn write_batched_records(
    State(components): State<Arc<Components>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    mut stream: BodyStream,
) -> Result<impl IntoResponse, HttpError> {
    let bucket_name = path.get("bucket_name").unwrap();
    check_permissions(
        &components,
        headers.clone(),
        WriteAccessPolicy {
            bucket: bucket_name.clone(),
        },
    )
    .await?;

    let entry_name = path.get("entry_name").unwrap();
    let record_headers: Vec<_> = sort_headers_by_name(&headers);
    let mut error_map = BTreeMap::new();

    let process_stream = async {
        let mut timed_headers: Vec<(u64, RecordHeader)> = Vec::new();
        for (k, v) in record_headers
            .iter()
            .filter(|(k, _)| k.as_str().starts_with("x-reduct-time-"))
        {
            let time = k[14..].parse::<u64>().map_err(|_| {
                ReductError::new(
                    ErrorCode::UnprocessableEntity,
                    &format!(
                        "Invalid header'{}': must be an unix timestamp in microseconds",
                        k
                    ),
                )
            })?;

            let header = parse_batched_header(v.to_str().unwrap())?;
            timed_headers.push((time, header));
        }

        check_content_length(&headers, &timed_headers)?;

        let mut senders = Vec::new();
        for (time, header) in &timed_headers {
            senders.push(
                start_writting(
                    &components,
                    bucket_name,
                    entry_name,
                    *time,
                    header,
                    &mut error_map,
                )
                .await,
            );
        }

        let mut index = 0;
        let mut written = 0;
        while let Some(chunk) = stream.next().await {
            let mut chunk = chunk?;

            while !chunk.is_empty() {
                match write_chunk(
                    &mut senders[index],
                    chunk,
                    &mut written,
                    timed_headers[index].1.content_length.clone(),
                )
                .await
                {
                    Ok(None) => {
                        chunk = Bytes::new();
                    }
                    Ok(Some(new_chunk)) => {
                        chunk = new_chunk;
                        index += 1;
                        written = 0;
                    }
                    Err(err) => {
                        return Err::<(), HttpError>(err.into());
                    }
                }
            }
        }

        if senders.len() < index {
            return Err(ReductError::bad_request("Content is shorter than expected").into());
        }

        Ok(())
    };

    if let Err(err) = process_stream.await {
        if !!headers.contains_key(Expect::name()) {
            debug!("draining the stream");
            while let Some(_) = stream.next().await {}
        }
        return Err::<HeaderMap, HttpError>(err);
    }

    let mut headers = HeaderMap::new();
    error_map.iter().for_each(|(time, err)| {
        headers.insert(
            HeaderName::from_str(&format!("x-reduct-error-{}", time)).unwrap(),
            HeaderValue::from_str(&format!("{},{}", err.status(), err.message())).unwrap(),
        );
    });

    Ok(headers.into())
}

async fn write_chunk(
    sender: &mut Sender<Result<Bytes, ReductError>>,
    chunk: Bytes,
    written: &mut usize,
    content_size: usize,
) -> Result<Option<Bytes>, ReductError> {
    let to_write = content_size - *written;
    *written += chunk.len();
    let (chunk, rest) = if chunk.len() < to_write {
        (chunk, None)
    } else {
        let chuck_to_write = chunk.slice(0..to_write);
        (chuck_to_write, Some(chunk.slice(to_write..)))
    };

    sender.send(Ok(chunk)).await.map_err(|_| {
        ReductError::new(
            ErrorCode::InternalServerError,
            "Internal reductstore error: failed to write to the storage",
        )
    })?;
    Ok(rest)
}

fn check_content_length(
    headers: &HeaderMap,
    timed_headers: &Vec<(u64, RecordHeader)>,
) -> Result<(), ReductError> {
    let total_content_length: usize = timed_headers
        .iter()
        .map(|(_, header)| header.content_length)
        .sum();

    if total_content_length
        != headers
            .get("content-length")
            .ok_or(ReductError::unprocessable_entity(
                "content-length header is required",
            ))?
            .to_str()
            .unwrap()
            .parse::<usize>()
            .map_err(|_| ReductError::unprocessable_entity("Invalid content-length header"))?
    {
        return Err(ReductError::unprocessable_entity(
            "content-length header does not match the sum of the content-lengths in the headers",
        )
        .into());
    }

    Ok(())
}

async fn start_writting(
    components: &Arc<Components>,
    bucket_name: &str,
    entry_name: &str,
    time: u64,
    record_header: &RecordHeader,
    error_map: &mut BTreeMap<u64, ReductError>,
) -> Sender<Result<Bytes, ReductError>> {
    let get_tx = async move {
        let mut storage = components.storage.write().await;
        let bucket = storage.get_mut_bucket(bucket_name)?;

        bucket
            .write_record(
                entry_name,
                time,
                record_header.content_length.clone(),
                record_header.content_type.clone(),
                record_header.labels.clone(),
            )
            .await
    };

    match get_tx.await {
        Ok(tx) => tx,
        Err(err) => {
            error_map.insert(time, err);
            // drain the stream
            let (tx, mut rx) = tokio::sync::mpsc::channel(1);
            tokio::spawn(async move { while let Some(_) = rx.recv().await {} });
            tx
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::entry::write_batched::write_batched_records;
    use crate::api::tests::{components, headers, path_to_entry_1};
    use crate::storage::proto::record::Label;
    use axum::body::Full;
    use axum::extract::FromRequest;
    use axum::http::Request;
    use rstest::{fixture, rstest};
    use tokio::time::sleep;

    #[rstest]
    #[tokio::test]
    async fn test_write_record_bad_timestamp(
        #[future] components: Arc<Components>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] body_stream: BodyStream,
    ) {
        headers.insert("content-length", "10".parse().unwrap());
        headers.insert("x-reduct-time-xxx", "10".parse().unwrap());

        let err = write_batched_records(
            State(components.await),
            headers,
            path_to_entry_1,
            body_stream.await,
        )
        .await
        .err()
        .unwrap();

        assert_eq!(
            err,
            HttpError::new(
                ErrorCode::UnprocessableEntity,
                "Invalid header'x-reduct-time-xxx': must be an unix timestamp in microseconds",
            )
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_batched_invalid_header(
        #[future] components: Arc<Components>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] body_stream: BodyStream,
    ) {
        headers.insert("content-length", "10".parse().unwrap());
        headers.insert("x-reduct-time-1", "".parse().unwrap());

        let err = write_batched_records(
            State(components.await),
            headers,
            path_to_entry_1,
            body_stream.await,
        )
        .await
        .err()
        .unwrap();

        assert_eq!(
            err,
            HttpError::new(ErrorCode::UnprocessableEntity, "Invalid batched header")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_batched_records(
        #[future] components: Arc<Components>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] body_stream: BodyStream,
    ) {
        let components = components.await;
        headers.insert("content-length", "48".parse().unwrap());
        headers.insert("x-reduct-time-1", "10,text/plain,a=b".parse().unwrap());
        headers.insert(
            "x-reduct-time-2",
            "20,text/plain,c=\"d,f\"".parse().unwrap(),
        );
        headers.insert("x-reduct-time-3", "18,text/plain".parse().unwrap());

        let stream = body_stream.await;

        write_batched_records(
            State(Arc::clone(&components)),
            headers,
            path_to_entry_1,
            stream,
        )
        .await
        .unwrap();

        sleep(std::time::Duration::from_millis(10)).await; // wait for the write to be done

        let storage = components.storage.read().await;
        let bucket = storage.get_bucket("bucket-1").unwrap();
        {
            let mut reader = bucket.begin_read("entry-1", 1).await.unwrap();
            assert_eq!(
                reader.labels()[0],
                Label {
                    name: "a".to_string(),
                    value: "b".to_string(),
                }
            );
            assert_eq!(reader.content_type(), "text/plain");
            assert_eq!(reader.content_length(), 10);
            assert_eq!(
                reader.rx().recv().await.unwrap(),
                Ok(Bytes::from("1234567890"))
            );
        }
        {
            let mut reader = bucket.begin_read("entry-1", 2).await.unwrap();
            assert_eq!(
                reader.labels()[0],
                Label {
                    name: "c".to_string(),
                    value: "d,f".to_string(),
                }
            );
            assert_eq!(reader.content_type(), "text/plain");
            assert_eq!(reader.content_length(), 20);
            assert_eq!(
                reader.rx().recv().await.unwrap(),
                Ok(Bytes::from("abcdef1234567890abcd"))
            );
        }
        {
            let mut reader = bucket.begin_read("entry-1", 3).await.unwrap();
            assert!(reader.labels().is_empty());
            assert_eq!(reader.content_type(), "text/plain");
            assert_eq!(reader.content_length(), 18);
            assert_eq!(
                reader.rx().recv().await.unwrap(),
                Ok(Bytes::from("ef1234567890abcdef"))
            );
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_batched_records_error(
        #[future] components: Arc<Components>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] body_stream: BodyStream,
    ) {
        let components = components.await;
        {
            let mut storage = components.storage.write().await;
            storage
                .get_mut_bucket("bucket-1")
                .unwrap()
                .write_record("entry-1", 2, 20, "text/plain".to_string(), HashMap::new())
                .await
                .unwrap();
        }

        headers.insert("content-length", "48".parse().unwrap());
        headers.insert("x-reduct-time-1", "10,".parse().unwrap());
        headers.insert("x-reduct-time-2", "20,".parse().unwrap());
        headers.insert("x-reduct-time-3", "18,".parse().unwrap());

        let stream = body_stream.await;

        let resp = write_batched_records(
            State(Arc::clone(&components)),
            headers,
            path_to_entry_1,
            stream,
        )
        .await
        .unwrap()
        .into_response();

        sleep(std::time::Duration::from_millis(10)).await; // wait for the write to be done

        let headers = resp.headers();
        assert_eq!(headers.len(), 1);
        assert_eq!(
            headers.get("x-reduct-error-2").unwrap(),
            &HeaderValue::from_static("409,A record with timestamp 2 already exists")
        );

        let storage = components.storage.read().await;
        let bucket = storage.get_bucket("bucket-1").unwrap();
        {
            let mut reader = bucket.begin_read("entry-1", 1).await.unwrap();
            assert_eq!(reader.content_length(), 10);
            assert_eq!(
                reader.rx().recv().await.unwrap(),
                Ok(Bytes::from("1234567890"))
            );
        }
        {
            let mut reader = bucket.begin_read("entry-1", 3).await.unwrap();
            assert_eq!(reader.content_length(), 18);
            assert_eq!(
                reader.rx().recv().await.unwrap(),
                Ok(Bytes::from("ef1234567890abcdef"))
            );
        }
    }

    #[fixture]
    async fn body_stream() -> BodyStream {
        let body = Full::new(Bytes::from(
            "1234567890abcdef1234567890abcdef1234567890abcdef",
        ));
        let request = Request::builder().body(body).unwrap();
        let stream = BodyStream::from_request(request, &()).await.unwrap();
        stream
    }
}
