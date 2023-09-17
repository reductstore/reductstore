// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::{Componentes, ErrorCode, HttpError};
use crate::auth::policy::WriteAccessPolicy;
use crate::storage::entry::Labels;
use crate::storage::writer::{Chunk, RecordWriter};
use axum::extract::{BodyStream, Path, Query, State};
use axum::headers::{Expect, Header, HeaderMap, HeaderValue};
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use log::{debug, error};
use reduct_base::batch::{parse_batched_header, sort_headers_by_name};
use reduct_base::error::ReductError;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use zip::write;

// POST /:bucket/:entry?ts=<number>
pub async fn write_record(
    State(components): State<Arc<Componentes>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
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
    // TODO: drain if there is an error , parse_batched_header must return Result, check total size of content-length
    let entry_name = path.get("entry_name").unwrap();
    let record_headers: Vec<_> = sort_headers_by_name(&headers);
    let mut record_headers: Vec<_> = record_headers
        .iter()
        .filter(|(k, _)| k.as_str().starts_with("x-reduct-time-"))
        .collect();
    let mut rest = BytesMut::new();
    while let Some(header) = record_headers.pop() {
        let writer = get_next_writer(
            &components,
            bucket_name,
            entry_name,
            header.0.as_str(),
            &header.1,
        )
        .await;
        match writer {
            Ok(writer) => {
                let mut writer = writer.write().unwrap();

                if rest.len() > 0 {
                    writer.write(Chunk::Data(rest.freeze()))?;
                    rest = BytesMut::new();
                }

                while let Some(chunk) = stream.next().await {
                    let chunk = chunk.unwrap();
                    let to_write = writer.content_length() - writer.written();
                    if chunk.len() < to_write {
                        writer.write(Chunk::Data(chunk))?;
                    } else {
                        let (to_write, for_next) = chunk.split_at(to_write);
                        writer.write(Chunk::Last(Bytes::from(Vec::from(to_write))))?;
                        rest = BytesMut::from(for_next);
                        break;
                    }
                }
            }
            Err(e) => {
                //todo: drain stream
                return Err(e);
            }
        }
    }

    Ok(())
}

async fn get_next_writer(
    components: &Arc<Componentes>,
    bucket_name: &str,
    entry_name: &str,
    name: &str,
    value: &HeaderValue,
) -> Result<Arc<RwLock<RecordWriter>>, HttpError> {
    let time = name.parse::<u64>().map_err(|_| {
        HttpError::new(
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

        let (content_length, content_type, labels) = parse_batched_header(value.to_str().unwrap());
        bucket.begin_write(entry_name, time, content_length, content_type, labels)
    };
    Ok(writer?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::entry::write_batched::write_record;
    use crate::api::tests::{components, empty_body, headers, path_to_entry_1};
    use axum::body::Empty;
    use axum::extract::FromRequest;
    use axum::http::Request;
    use rstest::{fixture, rstest};

    #[rstest]
    #[tokio::test]
    async fn test_write_record_bad_header(
        components: Arc<Componentes>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] empty_body: BodyStream,
    ) {
        headers.insert("content-length", "10".parse().unwrap());
        headers.insert("x-reduct-time-xxx", "10".parse().unwrap());

        let err = write_record(
            State(components),
            headers,
            path_to_entry_1,
            Query(HashMap::new()),
            empty_body.await,
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
}
