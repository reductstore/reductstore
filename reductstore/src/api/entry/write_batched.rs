// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::{Components, ErrorCode, HttpError};
use crate::auth::policy::WriteAccessPolicy;
use crate::storage::writer::{Chunk, RecordWriter, WriteChunk};
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
use std::fmt::format;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

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

        let mut writers: Vec<Arc<RwLock<dyn WriteChunk + Sync + Send>>> = Vec::new();
        for (time, header) in timed_headers {
            writers.push(
                get_writer(
                    &components,
                    bucket_name,
                    entry_name,
                    time,
                    header,
                    &mut error_map,
                )
                .await,
            );
        }

        let mut index = 0;
        while let Some(chunk) = stream.next().await {
            let mut chunk = chunk?;

            let write_chunk = |index: usize, chunk: Bytes| -> Result<Option<Bytes>, ReductError> {
                let mut writer = writers[index].write().unwrap();
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

            while !chunk.is_empty() {
                match write_chunk(index, chunk) {
                    Ok(None) => {
                        chunk = Bytes::new();
                    }
                    Ok(Some(new_chunk)) => {
                        chunk = new_chunk;
                        index += 1;
                    }
                    Err(err) => {
                        return Err::<(), HttpError>(err.into());
                    }
                }
            }
        }

        if writers.len() < index {
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
        return Err::<HeaderMap, HttpError>(err);
    }

    let mut headers = HeaderMap::new();
    error_map.iter().for_each(|(time, err)| {
        headers.insert(
            HeaderName::from_str(&format!("x-reduct-error-{}", time)).unwrap(),
            HeaderValue::from_str(&format!("{},\"{}\"", err.status(), err.message())).unwrap(),
        );
    });

    Ok(headers.into())
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

async fn get_writer(
    components: &Arc<Components>,
    bucket_name: &str,
    entry_name: &str,
    time: u64,
    record_header: RecordHeader,
    error_map: &mut BTreeMap<u64, ReductError>,
) -> Arc<RwLock<dyn WriteChunk + Send + Sync>> {
    let get_writer = async move {
        let mut storage = components.storage.write().await;
        let bucket = storage.get_mut_bucket(bucket_name)?;

        bucket.begin_write(
            entry_name,
            time,
            record_header.content_length,
            record_header.content_type,
            record_header.labels,
        )
    };

    match get_writer.await {
        Ok(writer) => writer,
        Err(err) => {
            error_map.insert(time, err);
            Arc::new(RwLock::new(ChunkDrainer::new(record_header.content_length)))
        }
    }
}

struct ChunkDrainer {
    written: usize,
    content_length: usize,
}

impl ChunkDrainer {
    fn new(content_length: usize) -> Self {
        Self {
            written: 0,
            content_length,
        }
    }
}

impl WriteChunk for ChunkDrainer {
    fn write(&mut self, chunk: Chunk) -> Result<(), ReductError> {
        match chunk {
            Chunk::Data(chunk) => {
                self.written += chunk.len();
                if self.written > self.content_length {
                    return Err(ReductError::bad_request(
                        "Content is bigger than in content-length",
                    ));
                }
            }
            Chunk::Last(chunk) => {
                self.written += chunk.len();
                if self.written > self.content_length {
                    return Err(ReductError::bad_request(
                        "Content is bigger than in content-length",
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn content_length(&self) -> usize {
        self.content_length
    }

    fn written(&self) -> usize {
        self.written
    }

    fn is_done(&self) -> bool {
        self.written == self.content_length
    }
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

    #[rstest]
    #[tokio::test]
    async fn test_write_batched_records_error(
        components: Arc<Components>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] body_stream: BodyStream,
    ) {
        {
            let mut storage = components.storage.write().await;
            storage
                .get_mut_bucket("bucket-1")
                .unwrap()
                .begin_write("entry-1", 2, 20, "text/plain".to_string(), HashMap::new())
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

        let headers = resp.headers();
        assert_eq!(headers.len(), 1);
        assert_eq!(
            headers.get("x-reduct-error-2").unwrap(),
            &HeaderValue::from_static("409,\"A record with timestamp 2 already exists\"")
        );

        let storage = components.storage.read().await;
        let bucket = storage.get_bucket("bucket-1").unwrap();
        {
            let reader = bucket.begin_read("entry-1", 1).unwrap();
            let mut reader = reader.write().unwrap();
            assert_eq!(reader.content_length(), 10);
            assert_eq!(reader.read().unwrap(), Some(Bytes::from("1234567890")));
        }
        {
            let reader = bucket.begin_read("entry-1", 3).unwrap();
            let mut reader = reader.write().unwrap();
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
