// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::{Components, HttpError};
use crate::auth::policy::WriteAccessPolicy;
use axum::body::{Body, BodyDataStream};
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum_extra::headers::{Expect, Header, HeaderMap};
use bytes::Bytes;
use futures_util::StreamExt;

use crate::api::entry::common::err_to_batched_header;
use crate::api::StateKeeper;
use crate::replication::{Transaction, TransactionNotification};
use crate::storage::bucket::Bucket;
use crate::storage::entry::RecordDrainer;
use log::{debug, error};
use reduct_base::batch::{parse_batched_header, sort_headers_by_time, RecordHeader};
use reduct_base::error::ReductError;
use reduct_base::io::{RecordMeta, WriteRecord};
use reduct_base::{bad_request, internal_server_error, unprocessable_entity};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tokio::time::timeout;

struct WriteContext {
    time: u64,
    header: RecordHeader,
    writer: Box<dyn WriteRecord + Sync + Send>,
}

type ErrorMap = BTreeMap<u64, ReductError>;

// POST /:bucket/:entry/batch
pub(super) async fn write_batched_records(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    body: Body,
) -> Result<impl IntoResponse, HttpError> {
    let bucket = path.get("bucket_name").unwrap();
    let components = keeper
        .get_with_permissions(&headers.clone(), WriteAccessPolicy { bucket })
        .await?;

    let entry_name = path.get("entry_name").unwrap().clone();
    let record_headers: Vec<_> = sort_headers_by_time(&headers)?;
    let mut stream = body.into_data_stream();

    let process_stream = async {
        let mut timed_headers: Vec<(u64, RecordHeader)> = Vec::new();
        for (time, v) in record_headers {
            let header = parse_batched_header(v.to_str().unwrap())?;
            timed_headers.push((time, header));
        }

        let content_length = check_and_get_content_length(&headers, &timed_headers)?;
        let (rx_writer, spawn_handler) =
            spawn_getting_writers(&components, &bucket, &entry_name, timed_headers).await?;
        receive_body_and_write_records(
            bucket,
            entry_name,
            components,
            content_length as i64,
            &mut stream,
            rx_writer,
        )
        .await?;

        Ok(spawn_handler
            .await
            .map_err(|e| internal_server_error!("Failed to complete write operation: {}", e))?)
    };

    match process_stream.await {
        Ok(error_map) => {
            let mut headers = HeaderMap::new();
            error_map.iter().for_each(|(time, err)| {
                err_to_batched_header(&mut headers, *time, err);
            });

            Ok(headers.into())
        }

        Err(err) => {
            if !headers.contains_key(Expect::name()) {
                debug!("draining the stream");
                while let Some(_) = stream.next().await {}
            }
            Err::<HeaderMap, HttpError>(err)
        }
    }
}

async fn notify_replication_write(
    components: &Arc<Components>,
    bucket: &str,
    entry_name: &str,
    ctx: &WriteContext,
) -> Result<(), ReductError> {
    components
        .replication_repo
        .write()
        .await?
        .notify(TransactionNotification {
            bucket: bucket.to_string(),
            entry: entry_name.to_string(),
            meta: RecordMeta::builder()
                .timestamp(ctx.time)
                .labels(ctx.header.labels.clone())
                .build(),
            event: Transaction::WriteRecord(ctx.time),
        })
        .await?;
    Ok(())
}

async fn receive_body_and_write_records(
    bucket: &String,
    entry_name: String,
    components: Arc<Components>,
    mut total_content_len: i64,
    stream: &mut BodyDataStream,
    mut rx_writer: Receiver<WriteContext>,
) -> Result<(), ReductError> {
    let mut chunk = Bytes::new();

    let mut read_chunk = async || -> Result<Bytes, ReductError> {
        if total_content_len == 0 {
            // it makes the code simpler to handle the last empty chunk case and empty records
            return Ok(Bytes::new());
        }
        match timeout(components.cfg.io_conf.operation_timeout, stream.next())
            .await
            .map_err(|_| bad_request!("Timeout while receiving data"))?
        {
            Some(Ok(data_chunk)) => {
                total_content_len -= data_chunk.len() as i64;
                Ok(data_chunk)
            }
            Some(Err(e)) => Err(bad_request!("Error while receiving data chunk: {}", e)),
            None => Err(bad_request!(
                "Content is shorter than expected: no more data to read"
            )),
        }
    };

    while let Some(mut ctx) = rx_writer.recv().await {
        let mut written = 0;

        if chunk.is_empty() {
            chunk = read_chunk().await?
        }

        loop {
            match write_chunk(
                &mut ctx.writer,
                chunk,
                &mut written,
                ctx.header.content_length.clone(),
                components.cfg.io_conf.operation_timeout,
            )
            .await
            {
                Ok(None) => {
                    // chunk is written but record is not finished yet
                    chunk = read_chunk().await?;
                    continue;
                }
                Ok(Some(rest)) => {
                    // finish writing the current record and start a new one
                    // finished writing the current record
                    if let Err(err) = ctx
                        .writer
                        .send_timeout(Ok(None), components.cfg.io_conf.operation_timeout)
                        .await
                    {
                        debug!("Timeout while sending EOF: {}", err);
                    }

                    notify_replication_write(&components, bucket, &entry_name, &ctx).await?;
                    chunk = rest;
                    break;
                }
                Err(err) => return Err(err),
            }
        }
    }

    Ok(())
}

async fn spawn_getting_writers(
    components: &Arc<Components>,
    bucket_name: &str,
    entry_name: &str,
    timed_headers: Vec<(u64, RecordHeader)>,
) -> Result<(Receiver<WriteContext>, JoinHandle<ErrorMap>), ReductError> {
    let (tx_writer, rx_writer) = tokio::sync::mpsc::channel(64);

    let bucket = components
        .storage
        .get_bucket(&bucket_name)
        .await?
        .upgrade_and_unwrap();

    let entry_name = entry_name.to_string();
    let spawn_handler = tokio::spawn(async move {
        let mut error_map = BTreeMap::new();

        for (time, header) in timed_headers.into_iter() {
            let writer =
                start_writing(&entry_name, bucket.clone(), time, &header, &mut error_map).await;

            tx_writer
                .send(WriteContext {
                    time,
                    header,
                    writer,
                })
                .await
                .map_err(|err| error!("Failed to send the writer: {}", err))
                .unwrap_or(());
        }
        error_map
    });

    Ok((rx_writer, spawn_handler))
}

async fn write_chunk(
    writer: &mut Box<dyn WriteRecord + Sync + Send>,
    chunk: Bytes,
    written: &mut usize,
    content_size: u64,
    io_timeout: std::time::Duration,
) -> Result<Option<Bytes>, ReductError> {
    let to_write = content_size - *written as u64;
    *written += chunk.len();
    let (chunk, rest) = if (chunk.len() as u64) < to_write {
        (chunk, None)
    } else {
        let chuck_to_write = chunk.slice(0..to_write as usize);
        (chuck_to_write, Some(chunk.slice(to_write as usize..)))
    };

    writer.send_timeout(Ok(Some(chunk)), io_timeout).await?;
    Ok(rest)
}

fn check_and_get_content_length(
    headers: &HeaderMap,
    timed_headers: &Vec<(u64, RecordHeader)>,
) -> Result<u64, ReductError> {
    let total_content_length: u64 = timed_headers
        .iter()
        .map(|(_, header)| header.content_length)
        .sum();

    if total_content_length
        != headers
            .get("content-length")
            .ok_or(unprocessable_entity!("content-length header is required",))?
            .to_str()
            .unwrap()
            .parse::<u64>()
            .map_err(|_| unprocessable_entity!("Invalid content-length header"))?
    {
        return Err(unprocessable_entity!(
            "content-length header does not match the sum of the content-lengths in the headers",
        )
        .into());
    }

    Ok(total_content_length)
}

async fn start_writing(
    entry_name: &str,
    bucket: Arc<Bucket>,
    time: u64,
    record_header: &RecordHeader,
    error_map: &mut BTreeMap<u64, ReductError>,
) -> Box<dyn WriteRecord + Sync + Send> {
    let get_writer = async {
        bucket
            .begin_write(
                entry_name,
                time,
                record_header.content_length.clone(),
                record_header.content_type.clone(),
                record_header.labels.clone(),
            )
            .await
    };

    match get_writer.await {
        Ok(writer) => writer,
        Err(err) => {
            error_map.insert(time, err);
            // drain the stream
            Box::new(RecordDrainer::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::entry::write_batched::write_batched_records;
    use crate::api::tests::{headers, keeper, path_to_entry_1};

    use axum_extra::headers::HeaderValue;
    use futures_util::stream;
    use reduct_base::error::ErrorCode;
    use reduct_base::io::ReadRecord;
    use rstest::{fixture, rstest};

    #[rstest]
    #[tokio::test]
    async fn test_write_record_bad_timestamp(
        #[future] keeper: Arc<StateKeeper>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] body_stream: Body,
    ) {
        headers.insert("content-length", "10".parse().unwrap());
        headers.insert("x-reduct-time-yyy", "10".parse().unwrap());

        let err = write_batched_records(
            State(keeper.await),
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
                "Invalid header 'x-reduct-time-yyy': must be an unix timestamp in microseconds",
            )
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_batched_invalid_header(
        #[future] keeper: Arc<StateKeeper>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] body_stream: Body,
    ) {
        headers.insert("content-length", "10".parse().unwrap());
        headers.insert("x-reduct-time-1", "".parse().unwrap());

        let err = write_batched_records(
            State(keeper.await),
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
        #[future] keeper: Arc<StateKeeper>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] body_stream: Body,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        headers.insert("content-length", "48".parse().unwrap());
        headers.insert("x-reduct-time-1", "10,text/plain,a=b".parse().unwrap());
        headers.insert(
            "x-reduct-time-2",
            "20,text/plain,c=\"d,f\"".parse().unwrap(),
        );
        headers.insert("x-reduct-time-10", "18,text/plain".parse().unwrap());

        let stream = body_stream.await;

        write_batched_records(State(Arc::clone(&keeper)), headers, path_to_entry_1, stream)
            .await
            .unwrap();

        let bucket = components
            .storage
            .get_bucket("bucket-1")
            .unwrap()
            .upgrade_and_unwrap();

        {
            let mut reader = bucket
                .get_entry("entry-1")
                .unwrap()
                .upgrade_and_unwrap()
                .begin_read(1)
                .await
                .unwrap();
            assert_eq!(&reader.meta().labels()["a"], "b");
            assert_eq!(reader.meta().content_type(), "text/plain");
            assert_eq!(reader.meta().content_length(), 10);
            assert_eq!(reader.read_chunk().unwrap(), Ok(Bytes::from("1234567890")));
        }
        {
            let mut reader = bucket
                .get_entry("entry-1")
                .unwrap()
                .upgrade_and_unwrap()
                .begin_read(2)
                .await
                .unwrap();
            assert_eq!(&reader.meta().labels()["c"], "d,f");
            assert_eq!(reader.meta().content_type(), "text/plain");
            assert_eq!(reader.meta().content_length(), 20);
            assert_eq!(
                reader.read_chunk().unwrap(),
                Ok(Bytes::from("abcdef1234567890abcd"))
            );
        }
        {
            let mut reader = bucket
                .get_entry("entry-1")
                .unwrap()
                .upgrade_and_unwrap()
                .begin_read(10)
                .await
                .unwrap();
            assert!(reader.meta().labels().is_empty());
            assert_eq!(reader.meta().content_type(), "text/plain");
            assert_eq!(reader.meta().content_length(), 18);
            assert_eq!(
                reader.read_chunk().unwrap(),
                Ok(Bytes::from("ef1234567890abcdef"))
            );
        }

        let info = components
            .replication_repo
            .read()
            .await
            .unwrap()
            .get_info("api-test")
            .unwrap();
        assert_eq!(info.info.pending_records, 3);
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_batched_records_with_empty_bodies(
        #[future] keeper: Arc<StateKeeper>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        headers.insert("content-length", "0".parse().unwrap());
        headers.insert("x-reduct-time-1", "0,,a=b".parse().unwrap());
        headers.insert("x-reduct-time-2", "0,,a=d".parse().unwrap());

        let stream = Body::empty();

        write_batched_records(State(Arc::clone(&keeper)), headers, path_to_entry_1, stream)
            .await
            .unwrap();

        let bucket = components
            .storage
            .get_bucket("bucket-1")
            .unwrap()
            .upgrade_and_unwrap();

        {
            let mut reader = bucket
                .get_entry("entry-1")
                .unwrap()
                .upgrade_and_unwrap()
                .begin_read(1)
                .await
                .unwrap();
            assert_eq!(reader.meta().content_length(), 0);
            assert_eq!(reader.read_chunk(), None);
        }
        {
            let mut reader = bucket
                .get_entry("entry-1")
                .unwrap()
                .upgrade_and_unwrap()
                .begin_read(2)
                .await
                .unwrap();
            assert_eq!(reader.meta().content_length(), 0);
            assert_eq!(reader.read_chunk(), None);
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_batched_records_complex(
        #[future] keeper: Arc<StateKeeper>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        headers.insert("content-length", "1000000".parse().unwrap());
        headers.insert("x-reduct-time-1", "500000,text/plain,a=b".parse().unwrap());
        headers.insert("x-reduct-time-2", "0,text/plain,a=c".parse().unwrap());
        headers.insert("x-reduct-time-3", "500000,text/plain,a=c".parse().unwrap());
        headers.insert("x-reduct-time-4", "0,text/plain,a=c".parse().unwrap());

        // the body will be split into 3 parts: 600000, 300000, 100000
        let stream = Body::from_stream(stream::iter(vec![
            Ok::<Bytes, ReductError>(Bytes::from(vec![0; 600000])),
            Ok(Bytes::from(vec![0; 300000])),
            Ok(Bytes::from(vec![0; 100000])),
        ]));

        write_batched_records(State(Arc::clone(&keeper)), headers, path_to_entry_1, stream)
            .await
            .unwrap();

        let bucket = components
            .storage
            .get_bucket("bucket-1")
            .unwrap()
            .upgrade_and_unwrap();

        {
            let reader = bucket
                .get_entry("entry-1")
                .unwrap()
                .upgrade_and_unwrap()
                .begin_read(1)
                .await
                .unwrap();
            assert_eq!(reader.meta().content_length(), 500000);
        }
        {
            let mut reader = bucket
                .get_entry("entry-1")
                .unwrap()
                .upgrade_and_unwrap()
                .begin_read(2)
                .await
                .unwrap();
            assert_eq!(reader.meta().content_length(), 0);
            assert_eq!(reader.read_chunk(), None);
        }
        {
            let reader = bucket
                .get_entry("entry-1")
                .unwrap()
                .upgrade_and_unwrap()
                .begin_read(3)
                .await
                .unwrap();
            assert_eq!(reader.meta().content_length(), 500000);
        }
        {
            let mut reader = bucket
                .get_entry("entry-1")
                .unwrap()
                .upgrade_and_unwrap()
                .begin_read(4)
                .await
                .unwrap();
            assert_eq!(reader.meta().content_length(), 0);
            assert_eq!(reader.read_chunk(), None);
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_batched_records_error(
        #[future] keeper: Arc<StateKeeper>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] body_stream: Body,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        {
            let mut writer = components
                .storage
                .get_bucket("bucket-1")
                .unwrap()
                .upgrade_and_unwrap()
                .begin_write("entry-1", 2, 20, "text/plain".to_string(), HashMap::new())
                .await
                .unwrap();
            writer
                .send(Ok(Some(Bytes::from(vec![0; 20]))))
                .await
                .unwrap();
            writer.send(Ok(None)).await.unwrap();
        }

        headers.insert("content-length", "48".parse().unwrap());
        headers.insert("x-reduct-time-1", "10,".parse().unwrap());
        headers.insert("x-reduct-time-2", "20,".parse().unwrap());
        headers.insert("x-reduct-time-3", "18,".parse().unwrap());

        let stream = body_stream.await;

        let resp =
            write_batched_records(State(Arc::clone(&keeper)), headers, path_to_entry_1, stream)
                .await
                .unwrap()
                .into_response();

        let headers = resp.headers();
        assert_eq!(headers.len(), 1);
        assert_eq!(
            headers.get("x-reduct-error-2").unwrap(),
            &HeaderValue::from_static("409,A record with timestamp 2 already exists")
        );

        let bucket = components
            .storage
            .get_bucket("bucket-1")
            .unwrap()
            .upgrade_and_unwrap();
        {
            let mut reader = bucket.begin_read("entry-1", 1).await.unwrap();
            assert_eq!(reader.meta().content_length(), 10);
            assert_eq!(reader.read_chunk().unwrap(), Ok(Bytes::from("1234567890")));
        }
        {
            let mut reader = bucket.begin_read("entry-1", 3).await.unwrap();
            assert_eq!(reader.meta().content_length(), 18);
            assert_eq!(
                reader.read_chunk().unwrap(),
                Ok(Bytes::from("ef1234567890abcdef"))
            );
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_batched_records_content_length_mismatch(
        #[future] keeper: Arc<StateKeeper>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
    ) {
        headers.insert("content-length", "60".parse().unwrap());
        headers.insert("x-reduct-time-1", "40,text/plain,a=b".parse().unwrap());
        headers.insert("x-reduct-time-2", "20,text/plain,c=d".parse().unwrap());
        let stream = Body::from("123456");
        let err = write_batched_records(
            State(Arc::clone(&keeper.await)),
            headers,
            path_to_entry_1,
            stream,
        )
        .await
        .err()
        .unwrap();

        let err: ReductError = err.into();
        assert_eq!(
            err,
            bad_request!("Content is shorter than expected: no more data to read")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_write_batched_records_errored_chunk(
        #[future] keeper: Arc<StateKeeper>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
    ) {
        headers.insert("content-length", "30".parse().unwrap());
        headers.insert("x-reduct-time-1", "10,text/plain,a=b".parse().unwrap());
        headers.insert("x-reduct-time-2", "20,text/plain,c=d".parse().unwrap());
        let stream = Body::from_stream(stream::iter(vec![
            Ok::<Bytes, ReductError>(Bytes::from("12345")),
            Err(bad_request!("Simulated chunk error")),
        ]));
        let err = write_batched_records(
            State(Arc::clone(&keeper.await)),
            headers,
            path_to_entry_1,
            stream,
        )
        .await
        .err()
        .unwrap();

        let err: ReductError = err.into();
        assert_eq!(
            err,
            bad_request!("Error while receiving data chunk: [BadRequest] Simulated chunk error")
        );
    }

    #[fixture]
    async fn body_stream() -> Body {
        Body::from("1234567890abcdef1234567890abcdef1234567890abcdef")
    }
}
