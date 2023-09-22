// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::{Components, ErrorCode, HttpError};
use crate::auth::policy::WriteAccessPolicy;
use crate::storage::writer::{Chunk, RecordWriter};
use axum::extract::{BodyStream, Path, State};
use axum::headers::{Expect, Header, HeaderMap, HeaderValue};
use bytes::Bytes;
use futures_util::StreamExt;
use log::debug;
use reduct_base::batch::{parse_batched_header, sort_headers_by_name};
use reduct_base::error::ReductError;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

// POST /:bucket/:entry/batch
pub async fn write_batched_records(
    State(components): State<Arc<Components>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    mut stream: BodyStream,
) -> Result<(), HttpError> {
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
    let record_headers: Vec<_> = record_headers
        .iter()
        .filter(|(k, _)| k.as_str().starts_with("x-reduct-time-"))
        .collect();

    let process_stream = async {
        let write_chunk = |writer_lock: Arc<RwLock<RecordWriter>>,
                           chunk: Bytes|
         -> Result<Option<Bytes>, ReductError> {
            let mut writer = writer_lock.write().unwrap();
            let to_write = writer.content_length() - writer.written();
            if chunk.len() < to_write {
                writer.write(Chunk::Data(chunk))?;
                Ok(None)
            } else {
                let chuck_to_write = chunk.slice(0..to_write);
                writer.write(Chunk::Last(chuck_to_write))?;
                Ok(Some(chunk.slice(to_write..)))
            }
        };

        let mut it = record_headers.iter();
        let mut current_writer: Option<_> = None;
        while let Some(chunk) = stream.next().await {
            let mut chunk = chunk?;

            while !chunk.is_empty() {
                if current_writer.is_none() {
                    let (name, value) = it
                        .next()
                        .ok_or(ReductError::bad_request("Content is longer than expected"))?;
                    current_writer = Some(
                        get_writer(&components, bucket_name, entry_name, name.as_str(), value)
                            .await?,
                    );
                }

                match write_chunk(current_writer.clone().unwrap(), chunk) {
                    Ok(None) => {
                        chunk = Bytes::new();
                    }
                    Ok(Some(new_chunk)) => {
                        chunk = new_chunk;
                        current_writer = None;
                    }
                    Err(err) => {
                        return Err(err.into());
                    }
                }
            }
        }

        if it.next().is_some() {
            return Err(ReductError::bad_request("Content is shorter than expected").into());
        }

        Ok(())
    };

    if let Err(err) = process_stream.await {
        if !headers
            .get(Expect::name())
            .eq(&Some(&HeaderValue::from_static("100-continue")))
        {
            debug!("draining the stream");
            while let Some(_) = stream.next().await {}
        }
        return Err(err);
    }

    Ok(())
}

async fn get_writer(
    components: &Arc<Components>,
    bucket_name: &str,
    entry_name: &str,
    name: &str,
    value: &HeaderValue,
) -> Result<Arc<RwLock<RecordWriter>>, ReductError> {
    let time = name[14..].parse::<u64>().map_err(|_| {
        ReductError::new(
            ErrorCode::UnprocessableEntity,
            &format!(
                "Invalid header'{}': must be an unix timestamp in microseconds",
                name
            ),
        )
    })?;

    let writer = {
        let mut storage = components.storage.write().await;
        let bucket = storage.get_mut_bucket(bucket_name)?;

        let (content_length, content_type, labels) = parse_batched_header(value.to_str().unwrap())?;
        bucket.begin_write(entry_name, time, content_length, content_type, labels)
    };
    Ok(writer?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::entry::write_batched::write_batched_records;
    use crate::api::tests::{components, headers, path_to_entry_1};
    use axum::body::Full;
    use axum::extract::FromRequest;
    use axum::http::Request;
    use rstest::{fixture, rstest};

    #[rstest]
    #[tokio::test]
    async fn test_write_record_bad_timestamp(
        components: Arc<Components>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] body_stream: BodyStream,
    ) {
        headers.insert("content-length", "10".parse().unwrap());
        headers.insert("x-reduct-time-xxx", "10".parse().unwrap());

        let err = write_batched_records(
            State(components),
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
        components: Arc<Components>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] body_stream: BodyStream,
    ) {
        headers.insert("content-length", "10".parse().unwrap());
        headers.insert("x-reduct-time-1", "".parse().unwrap());

        let err = write_batched_records(
            State(components),
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
        components: Arc<Components>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] body_stream: BodyStream,
    ) {
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

        let storage = components.storage.read().await;
        let bucket = storage.get_bucket("bucket-1").unwrap();
        {
            let reader = bucket.begin_read("entry-1", 1).unwrap();
            let mut reader = reader.write().unwrap();
            assert_eq!(reader.labels().get("a"), Some(&"b".to_string()));
            assert_eq!(reader.content_type(), "text/plain");
            assert_eq!(reader.content_length(), 10);
            assert_eq!(reader.read().unwrap(), Some(Bytes::from("1234567890")));
        }
        {
            let reader = bucket.begin_read("entry-1", 2).unwrap();
            let mut reader = reader.write().unwrap();
            assert_eq!(reader.labels().get("c"), Some(&"d,f".to_string()));
            assert_eq!(reader.content_type(), "text/plain");
            assert_eq!(reader.content_length(), 20);
            assert_eq!(
                reader.read().unwrap(),
                Some(Bytes::from("abcdef1234567890abcd"))
            );
        }
        {
            let reader = bucket.begin_read("entry-1", 3).unwrap();
            let mut reader = reader.write().unwrap();
            assert!(reader.labels().is_empty());
            assert_eq!(reader.content_type(), "text/plain");
            assert_eq!(reader.content_length(), 18);
            assert_eq!(
                reader.read().unwrap(),
                Some(Bytes::from("ef1234567890abcdef"))
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
